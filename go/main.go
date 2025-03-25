package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/xuri/excelize/v2"
	"golang.org/x/sync/semaphore"
)

const (
	twoCaptchaAPIKey  = "6b839fc1d6dd5a9a77261a4fdc2aeb1f"
	twoCaptchaAPIURL  = "https://2captcha.com/in.php"
	twoCaptchaResURL  = "https://2captcha.com/res.php"
	baseURL           = "https://muisca.dian.gov.co/WebRutMuisca/DefConsultaEstadoRUT.faces"
	maxRetries        = 3
	captchaRetryDelay = 5 * time.Second
	userAgent         = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

type Config struct {
	APIKey              string
	Concurrency         int
	BatchSize           int
	MaxParallelBrowsers int
	UseGPU              bool
	TimeoutConfig
	ProxyList []string
}

type TimeoutConfig struct {
	Initial        time.Duration
	DataExtraction time.Duration
	Captcha        time.Duration
	RetryDelay     time.Duration
	MaxRetries     int
}

type Result struct {
	Cedula          string `json:"cedula"`
	PrimerApellido  string `json:"primerApellido"`
	SegundoApellido string `json:"segundoApellido"`
	PrimerNombre    string `json:"primerNombre"`
	SegundoNombre   string `json:"segundoNombre"`
	Estado          string `json:"estado"`
	Attempts        int    `json:"attempts"`
	Error           string `json:"error,omitempty"`
	ProcessingTime  string `json:"processingTime,omitempty"`
	Screenshot      []byte `json:"-"` // No incluir en JSON
}

type CaptchaResponse struct {
	Status  int    `json:"status"`
	Request string `json:"request"`
}

type Scraper struct {
	config     Config
	rootCtx    context.Context
	rootCancel context.CancelFunc
	sem        *semaphore.Weighted
	results    chan Result
	wg         sync.WaitGroup
}

func NewScraper(config Config) (*Scraper, error) {
	// Crear contexto raíz con timeout global
	rootCtx, rootCancel := context.WithCancel(context.Background())

	// Configurar opciones de Chrome
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.WindowSize(1920, 1080),
		chromedp.UserAgent(userAgent),
		chromedp.Flag("headless", false), // Mostrar navegador para depuración
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-default-apps", true),
		chromedp.Flag("disable-popup-blocking", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-background-timer-throttling", true),
		chromedp.Flag("disable-backgrounding-occluded-windows", true),
		chromedp.Flag("disable-breakpad", true),
		chromedp.Flag("disable-client-side-phishing-detection", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-infobars", true),
		chromedp.Flag("disable-notifications", true),
		chromedp.Flag("disable-translate", true),
		chromedp.Flag("enable-automation", false),
		chromedp.Flag("no-sandbox", true),
	)

	// Add GPU option if needed
	if !config.UseGPU {
		opts = append(opts, chromedp.DisableGPU)
	}

	// Crear allocator con las opciones
	allocCtx, _ := chromedp.NewExecAllocator(rootCtx, opts...)

	return &Scraper{
		config:     config,
		rootCtx:    allocCtx,
		rootCancel: rootCancel,
		sem:        semaphore.NewWeighted(int64(config.Concurrency)),
		results:    make(chan Result, config.Concurrency*2),
	}, nil
}

func (s *Scraper) ProcessCedulas(cedulas []string) []Result {
	results := make([]Result, len(cedulas))
	resultsMutex := &sync.Mutex{}

	log.Printf("Procesando %d cédulas", len(cedulas))

	// Crear mapa de índices
	cedulaIndices := make(map[string]int, len(cedulas))
	for i, cedula := range cedulas {
		cedulaIndices[cedula] = i
	}

	// Calcular el número óptimo de navegadores basado en el número de CPUs
	optimalBrowsers := runtime.NumCPU()
	if s.config.MaxParallelBrowsers > 0 && s.config.MaxParallelBrowsers < optimalBrowsers {
		optimalBrowsers = s.config.MaxParallelBrowsers
	}
	log.Printf("Usando %d navegadores en paralelo", optimalBrowsers)

	// Dividir las cédulas en grupos para los workers
	cedulasPerBrowser := (len(cedulas) + optimalBrowsers - 1) / optimalBrowsers
	log.Printf("Cédulas por navegador: %d", cedulasPerBrowser)

	// Iniciar workers
	for i := 0; i < optimalBrowsers && i*cedulasPerBrowser < len(cedulas); i++ {
		startIdx := i * cedulasPerBrowser
		endIdx := (i + 1) * cedulasPerBrowser
		if endIdx > len(cedulas) {
			endIdx = len(cedulas)
		}

		log.Printf("Iniciando worker %d para procesar cédulas %d-%d", i, startIdx, endIdx-1)
		s.wg.Add(1)
		go s.worker(cedulas[startIdx:endIdx], i)
	}

	// Recolector de resultados
	go func() {
		for result := range s.results {
			if idx, ok := cedulaIndices[result.Cedula]; ok {
				resultsMutex.Lock()
				results[idx] = result
				resultsMutex.Unlock()
				log.Printf("Resultado recibido para cédula %s: %s", result.Cedula, result.Estado)
			}
		}
	}()

	s.wg.Wait()
	close(s.results)
	log.Printf("Todos los workers han terminado")

	return results
}

func (s *Scraper) worker(cedulas []string, browserIdx int) {
	defer s.wg.Done()

	log.Printf("Worker %d iniciado con %d cédulas", browserIdx, len(cedulas))

	// Crear un contexto para este navegador
	browserCtx, cancel := chromedp.NewContext(s.rootCtx,
		chromedp.WithLogf(log.Printf),
	)
	defer cancel()

	// Iniciar el navegador para este worker
	log.Printf("Worker %d: Iniciando navegador", browserIdx)
	err := chromedp.Run(browserCtx,
		chromedp.Navigate("about:blank"),
	)

	if err != nil {
		log.Printf("Worker %d: Error iniciando navegador: %v", browserIdx, err)
		// Marcar todas las cédulas asignadas como error
		for _, cedula := range cedulas {
			s.results <- Result{
				Cedula:   cedula,
				Estado:   "Error",
				Error:    fmt.Sprintf("Error iniciando navegador: %v", err),
				Attempts: 1,
			}
		}
		return
	}

	log.Printf("Worker %d: Navegador iniciado correctamente", browserIdx)

	for _, cedula := range cedulas {
		log.Printf("Worker %d procesando cédula: %s", browserIdx, cedula)
		if err := s.sem.Acquire(context.Background(), 1); err != nil {
			log.Printf("Error adquiriendo semáforo: %v", err)
			continue
		}

		// Procesar con reintentos
		var result Result
		for attempt := 1; attempt <= s.config.TimeoutConfig.MaxRetries; attempt++ {
			result = s.processCedula(cedula, browserCtx, attempt)
			if result.Error == "" || !strings.Contains(result.Error, "captcha") {
				break
			}
			log.Printf("Reintentando cédula %s (intento %d) debido a error de captcha", cedula, attempt)
			time.Sleep(s.config.TimeoutConfig.RetryDelay)
		}

		s.results <- result
		log.Printf("Worker %d completó cédula %s con estado: %s", browserIdx, cedula, result.Estado)

		s.sem.Release(1)
	}

	log.Printf("Worker %d ha terminado", browserIdx)
}

func (s *Scraper) processCedula(cedula string, ctx context.Context, attempt int) Result {
	startTime := time.Now()
	result := Result{Cedula: cedula, Attempts: attempt}

	log.Printf("Iniciando consulta para cédula: %s (intento %d)", cedula, attempt)

	// Create a new tab
	tabCtx, cancel := chromedp.NewContext(ctx)
	defer cancel()

	// Set timeout más largo
	timeoutCtx, timeoutCancel := context.WithTimeout(tabCtx, 60*time.Second)
	defer timeoutCancel()

	// Navegar a la página e introducir la cédula
	err := chromedp.Run(timeoutCtx,
		// Limpiar cookies y caché
		network.ClearBrowserCookies(),
		network.ClearBrowserCache(),
		// Navegar a la página principal
		chromedp.Navigate(baseURL),
		// Esperar a que la página cargue completamente (5 segundos)
		chromedp.Sleep(5*time.Second),
		// Verificar que el campo de cédula esté visible
		chromedp.WaitVisible(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:numNit"]`, chromedp.BySearch),
		// Introducir la cédula
		chromedp.Clear(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:numNit"]`, chromedp.BySearch),
		chromedp.SendKeys(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:numNit"]`, cedula, chromedp.BySearch),
		// Esperar 5 segundos como indica el usuario
		chromedp.Sleep(5*time.Second),
	)

	if err != nil {
		log.Printf("Error al navegar o introducir cédula %s: %v", cedula, err)
		result.Error = fmt.Sprintf("Error al navegar: %v", err)
		result.Estado = "Error"
		result.ProcessingTime = time.Since(startTime).String()
		return result
	}

	// Verificar si hay captcha y resolverlo
	var captchaVisible bool
	err = chromedp.Run(timeoutCtx,
		chromedp.Evaluate(`document.querySelector('//*[@id="verifying"]') !== null`, &captchaVisible),
	)

	if captchaVisible {
		log.Printf("Captcha detectado para cédula %s", cedula)

		// Capturar imagen del captcha
		var captchaImg []byte
		err = chromedp.Run(timeoutCtx,
			chromedp.Screenshot(`//*[@id="verifying"]`, &captchaImg, chromedp.NodeVisible),
		)

		if err != nil {
			log.Printf("Error capturando imagen del captcha: %v", err)
			result.Error = fmt.Sprintf("Error con captcha: %v", err)
			result.Estado = "Error"
			result.ProcessingTime = time.Since(startTime).String()
			return result
		}

		// Guardar imagen del captcha para debugging
		os.WriteFile(fmt.Sprintf("captcha_%s.png", cedula), captchaImg, 0644)

		// Resolver captcha usando 2captcha
		captchaText, err := solveCaptcha(captchaImg)
		if err != nil {
			log.Printf("Error resolviendo captcha: %v", err)
			result.Error = fmt.Sprintf("Error resolviendo captcha: %v", err)
			result.Estado = "Error"
			result.ProcessingTime = time.Since(startTime).String()
			return result
		}

		log.Printf("Captcha resuelto para cédula %s: %s", cedula, captchaText)

		// Introducir el captcha en el campo correspondiente
		err = chromedp.Run(timeoutCtx,
			chromedp.WaitVisible(`//*[@id="verifying"]`, chromedp.BySearch),
			chromedp.SendKeys(`//*[@id="verifying"]`, captchaText, chromedp.BySearch),
			chromedp.Sleep(1*time.Second),
		)

		if err != nil {
			log.Printf("Error introduciendo captcha: %v", err)
			result.Error = fmt.Sprintf("Error con captcha: %v", err)
			result.Estado = "Error"
			result.ProcessingTime = time.Since(startTime).String()
			return result
		}
	}

	// Hacer clic en el botón de búsqueda
	err = chromedp.Run(timeoutCtx,
		chromedp.WaitVisible(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:btnBuscar"]`, chromedp.BySearch),
		chromedp.Click(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:btnBuscar"]`, chromedp.BySearch),
		chromedp.Sleep(5*time.Second), // Esperar a que carguen los resultados
	)

	if err != nil {
		log.Printf("Error haciendo clic en el botón de búsqueda: %v", err)
		result.Error = fmt.Sprintf("Error en botón búsqueda: %v", err)
		result.Estado = "Error"
		result.ProcessingTime = time.Since(startTime).String()
		return result
	}

	// Comprobar si hay mensaje de error
	var errorMessage string
	var hasError bool
	_ = chromedp.Run(timeoutCtx,
		chromedp.Evaluate(`document.querySelector('.ui-messages-error-summary') !== null`, &hasError),
	)

	if hasError {
		_ = chromedp.Run(timeoutCtx,
			chromedp.Text(`.ui-messages-error-summary`, &errorMessage, chromedp.ByQuery),
		)
		log.Printf("Error en la consulta de la cédula %s: %s", cedula, errorMessage)
		result.Error = errorMessage
		result.Estado = "Error"
		result.ProcessingTime = time.Since(startTime).String()
		return result
	}

	// Extraer los datos de los campos especificados
	var numNit, primerApellido, primerNombre, segundoApellido, otrosNombres, estado string
	err = chromedp.Run(timeoutCtx,
		chromedp.Text(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:numNit"]`, &numNit, chromedp.BySearch),
		chromedp.Text(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:primerApellido"]`, &primerApellido, chromedp.BySearch),
		chromedp.Text(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:primerNombre"]`, &primerNombre, chromedp.BySearch),
		chromedp.Text(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:segundoApellido"]`, &segundoApellido, chromedp.BySearch),
		chromedp.Text(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:otrosNombres"]`, &otrosNombres, chromedp.BySearch),
		chromedp.Text(`//*[@id="vistaConsultaEstadoRUT:formConsultaEstadoRUT:estado"]`, &estado, chromedp.BySearch),
	)

	if err != nil {
		log.Printf("Error extrayendo datos: %v", err)
		result.Error = fmt.Sprintf("Error extrayendo datos: %v", err)
		result.Estado = "Error"
		result.ProcessingTime = time.Since(startTime).String()
		return result
	}

	// Asignar los valores extraídos al resultado
	result.PrimerApellido = primerApellido
	result.SegundoApellido = segundoApellido
	result.PrimerNombre = primerNombre
	result.SegundoNombre = otrosNombres
	result.Estado = estado

	log.Printf("Datos extraídos para cédula %s: Nombre: %s %s %s %s, Estado: %s",
		cedula, primerNombre, otrosNombres, primerApellido, segundoApellido, estado)

	result.ProcessingTime = time.Since(startTime).String()
	return result
}

// Resolver captcha usando el servicio 2captcha
func solveCaptcha(captchaImg []byte) (string, error) {
	// Codificar la imagen en base64
	base64Img := base64.StdEncoding.EncodeToString(captchaImg)

	// Construir la solicitud para enviar a 2captcha
	formData := url.Values{}
	formData.Set("key", twoCaptchaAPIKey)
	formData.Set("method", "base64")
	formData.Set("body", base64Img)
	formData.Set("json", "1")

	// Enviar solicitud para resolver captcha
	resp, err := http.PostForm(twoCaptchaAPIURL, formData)
	if err != nil {
		return "", fmt.Errorf("error enviando captcha a 2captcha: %v", err)
	}
	defer resp.Body.Close()

	// Leer respuesta
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error leyendo respuesta de 2captcha: %v", err)
	}

	// Parsear respuesta JSON
	var captchaResp CaptchaResponse
	if err := json.Unmarshal(body, &captchaResp); err != nil {
		return "", fmt.Errorf("error parseando respuesta de 2captcha: %v", err)
	}

	if captchaResp.Status != 1 {
		return "", fmt.Errorf("error en respuesta de 2captcha: %s", captchaResp.Request)
	}

	captchaID := captchaResp.Request

	// Esperar a que el captcha sea resuelto
	for i := 0; i < 30; i++ { // Máximo 30 intentos (150 segundos)
		time.Sleep(captchaRetryDelay)

		// Consultar resultado del captcha
		checkURL := fmt.Sprintf("%s?key=%s&action=get&id=%s&json=1",
			twoCaptchaResURL, twoCaptchaAPIKey, captchaID)

		resp, err := http.Get(checkURL)
		if err != nil {
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		var resultResp CaptchaResponse
		if err := json.Unmarshal(body, &resultResp); err != nil {
			continue
		}

		if resultResp.Status == 1 {
			return resultResp.Request, nil
		}

		// Si la respuesta es "CAPCHA_NOT_READY", seguimos esperando
		if resultResp.Request != "CAPCHA_NOT_READY" {
			return "", fmt.Errorf("error resolviendo captcha: %s", resultResp.Request)
		}
	}

	return "", fmt.Errorf("timeout esperando resolución del captcha")
}

func (s *Scraper) Close() {
	s.rootCancel()
	log.Printf("Scraper cerrado")
}

func getDefaultConfig() Config {
	numCPU := runtime.NumCPU()
	return Config{
		APIKey:              twoCaptchaAPIKey,
		Concurrency:         numCPU * 2,
		BatchSize:           100,
		MaxParallelBrowsers: numCPU,
		UseGPU:              true,
		TimeoutConfig: TimeoutConfig{
			Initial:        60 * time.Second,
			DataExtraction: 30 * time.Second,
			Captcha:        60 * time.Second,
			RetryDelay:     5 * time.Second,
			MaxRetries:     3,
		},
	}
}

func writeResultsToExcel(filename string, results []Result) error {
	f := excelize.NewFile()
	sheet := "Results"
	f.NewSheet(sheet)

	// Write headers
	headers := []string{"Cedula", "Primer Apellido", "Segundo Apellido", "Primer Nombre", "Segundo Nombre", "Estado", "Intentos", "Error", "Tiempo"}
	for i, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue(sheet, cell, header)
	}

	// Write data
	for i, result := range results {
		row := i + 2
		f.SetCellValue(sheet, fmt.Sprintf("A%d", row), result.Cedula)
		f.SetCellValue(sheet, fmt.Sprintf("B%d", row), result.PrimerApellido)
		f.SetCellValue(sheet, fmt.Sprintf("C%d", row), result.SegundoApellido)
		f.SetCellValue(sheet, fmt.Sprintf("D%d", row), result.PrimerNombre)
		f.SetCellValue(sheet, fmt.Sprintf("E%d", row), result.SegundoNombre)
		f.SetCellValue(sheet, fmt.Sprintf("F%d", row), result.Estado)
		f.SetCellValue(sheet, fmt.Sprintf("G%d", row), result.Attempts)
		f.SetCellValue(sheet, fmt.Sprintf("H%d", row), result.Error)
		f.SetCellValue(sheet, fmt.Sprintf("I%d", row), result.ProcessingTime)
	}

	return f.SaveAs(filename)
}

func readCedulasFromExcel(filename string) ([]string, error) {
	f, err := excelize.OpenFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error abriendo archivo Excel: %v", err)
	}
	defer f.Close()

	// Obtener todas las filas de la primera hoja
	rows, err := f.GetRows(f.GetSheetName(0))
	if err != nil {
		return nil, fmt.Errorf("error leyendo filas: %v", err)
	}

	cedulas := make([]string, 0, len(rows))
	for i, row := range rows {
		if i == 0 { // Saltar fila de encabezado
			continue
		}
		if len(row) > 0 {
			// Limpiar la cédula para asegurar que no tenga espacios o caracteres no válidos
			cedula := strings.TrimSpace(row[0])
			if cedula != "" {
				cedulas = append(cedulas, cedula)
			}
		}
	}

	return cedulas, nil
}

func main() {
	// Configuración optimizada para grandes volúmenes
	config := getDefaultConfig()

	// Para procesar 18,000 cédulas, ajustamos algunos parámetros
	config.MaxParallelBrowsers = runtime.NumCPU() // Usar todos los CPUs disponibles
	config.Concurrency = runtime.NumCPU() * 2     // Concurrencia ajustada

	// Utilizar todo el potencial de la CPU
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Printf("Iniciando scraper con %d navegadores en paralelo", config.MaxParallelBrowsers)

	scraper, err := NewScraper(config)
	if err != nil {
		log.Fatalf("Error inicializando scraper: %v", err)
	}
	defer scraper.Close()

	// Leer archivo de entrada
	inputFile := "/Users/alpadev/Desktop/Scrapper/js/test.xlsx"
     // Cambiar al nombre del archivo con las 18,000 cédulas
	log.Printf("Leyendo cédulas del archivo: %s", inputFile)

	cedulas, err := readCedulasFromExcel(inputFile)
	if err != nil {
		log.Fatalf("Error leyendo cédulas: %v", err)
	}

	log.Printf("Se leyeron %d cédulas del archivo", len(cedulas))

	// Procesar cédulas
	startTime := time.Now()
	log.Printf("Iniciando procesamiento de %d cédulas", len(cedulas))

	results := scraper.ProcessCedulas(cedulas)
	duration := time.Since(startTime)

	// Guardar resultados
	outputFile := "resultados_consulta.xlsx"
	if err := writeResultsToExcel(outputFile, results); err != nil {
		log.Printf("Error guardando resultados: %v", err)
	} else {
		log.Printf("Resultados guardados en: %s", outputFile)
	}

	// Estadísticas
	var successful, errors, noData int
	for _, result := range results {
		if result.Error == "" && result.Estado != "" {
			successful++
		} else if result.Error != "" {
			errors++
		} else {
			noData++
		}
	}

	log.Printf("=== RESUMEN DE PROCESAMIENTO ===")
	log.Printf("Total de cédulas procesadas: %d", len(cedulas))
	log.Printf("Consultas exitosas: %d (%.2f%%)", successful, float64(successful)/float64(len(cedulas))*100)
	log.Printf("Consultas con error: %d (%.2f%%)", errors, float64(errors)/float64(len(cedulas))*100)
	log.Printf("Consultas sin datos: %d (%.2f%%)", noData, float64(noData)/float64(len(cedulas))*100)
	log.Printf("Tiempo total de procesamiento: %v", duration)
	log.Printf("Promedio por cédula: %v", duration/time.Duration(len(cedulas)))
	log.Printf("================================")
}
