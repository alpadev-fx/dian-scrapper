import puppeteer from "puppeteer";
import fs from "fs";
import axios from "axios";
import XLSX from "xlsx";

const TWO_CAPTCHA_API_URL = "https://2captcha.com";

async function checkBalance(apiKey) {
  try {
    const response = await axios.get(`${TWO_CAPTCHA_API_URL}/res.php`, {
      params: { key: apiKey, action: "getbalance", json: 1 },
    });
    console.log("Account balance:", response.data);
    return parseFloat(response.data.request);
  } catch (error) {
    console.error("Error checking balance:", error.message);
    return null;
  }
}

async function solveTurnstileWith2Captcha(siteKey, apiKey, pageUrl) {
  const maxAttempts = 20;
  const initialDelay = 1000; 
  const timeoutPerAttempt = 3000;

  try {
    console.log("Submitting Turnstile captcha to 2Captcha...");
    const submitResponse = await axios.get(`${TWO_CAPTCHA_API_URL}/in.php`, {
      params: {
        key: apiKey,
        method: "turnstile",
        sitekey: siteKey,
        pageurl: pageUrl,
        json: 1,
      },
      timeout: 2000,
    });

    if (!submitResponse.data.status || submitResponse.data.status !== 1) {
      throw new Error(`2Captcha submission error: ${submitResponse.data.request}`);
    }
    const captchaId = submitResponse.data.request;
    console.log(`Captcha submitted successfully. ID: ${captchaId}`);

    let token = null;
    let attempts = 0;
    let delay = initialDelay;

    while (attempts < maxAttempts) {
      attempts++;
      await new Promise((resolve) => setTimeout(resolve, delay));
      console.log(`Checking captcha solution (attempt ${attempts}/${maxAttempts})...`);
      try {
        const resultResponse = await axios.get(`${TWO_CAPTCHA_API_URL}/res.php`, {
          params: { key: apiKey, action: "get", id: captchaId, json: 1 },
          timeout: timeoutPerAttempt,
        });
        if (resultResponse.data.status === 1) {
          token = resultResponse.data.request;
          console.log("Captcha solved successfully!");
          break;
        } else if (resultResponse.data.request === "CAPCHA_NOT_READY") {
          delay = Math.min(delay * 2, 2000);
          continue;
        } else if (resultResponse.data.request.includes("ERROR_CAPTCHA_UNSOLVABLE")) {
          console.error("2Captcha reports unsolvable captcha. Aborting.");
          return null;
        } else {
          console.warn(`2Captcha error: ${resultResponse.data.request}. Retrying...`);
          delay = Math.min(delay * 2, 2000);
          continue;
        }
      } catch (axiosError) {
        if (axiosError.code === "ECONNABORTED") {
          console.error(`Timeout checking captcha solution (attempt ${attempts}). Retrying...`);
        } else {
          console.error(`Error checking captcha solution (attempt ${attempts}):`, axiosError.message);
        }
        delay = Math.min(delay * 2, 2000);
        continue;
      }
    }

    if (!token) {
      throw new Error("Captcha solving timed out or reached max attempts");
    }
    return token;
  } catch (error) {
    console.error("Error solving captcha with 2Captcha:", error.message);
    return null;
  }
}

function readCedulasFromExcel(filePath) {
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
  return rows.map((row) => String(row[0] || "").trim()).filter(Boolean);
}

async function optimizeRequestInterception(page) {
  await page.setRequestInterception(true);
  page.on("request", (request) => {
    const resourceType = request.resourceType();
    if (["image", "stylesheet", "font"].includes(resourceType)) {
      request.abort();
    } else {
      request.continue();
    }
  });
}

async function injectTurnstileToken(page, token) {
  await page.evaluate((token) => {
    const turnstileFields = Array.from(
      document.querySelectorAll('input[name*="cf-turnstile"], input[name*="turnstile"], input[name*="captcha"]')
    );
    if (turnstileFields.length > 0) {
      turnstileFields.forEach((field) => (field.value = token));
    }
    const forms = document.querySelectorAll("form");
    forms.forEach((form) => {
      let tokenInput = form.querySelector('[name="cf-turnstile-response"]');
      if (!tokenInput) {
        tokenInput = document.createElement("input");
        tokenInput.id = "cf-turnstile-response";
        tokenInput.type = "hidden";
        tokenInput.name = "cf-turnstile-response";
        form.appendChild(tokenInput);
      }
      tokenInput.value = token;
    });
    if (typeof window.turnstileCallback === "function") {
      window.turnstileCallback(token);
    }
    window.turnstileToken = token;
    document.querySelectorAll(".cf-turnstile").forEach((el) => el.setAttribute("data-token", token));
  }, token);
}

async function extractDataFromPage(page) {
  return page.$$eval(
    "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT",
    (elements) => {
      if (elements.length === 0) {
        return null;
      }
      const element = elements[0];
      return {
        primerApellido:
          element.querySelector("#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:primerApellido")?.innerText.trim() || "N/A",
        segundoApellido:
          element.querySelector("#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:segundoApellido")?.innerText.trim() || "N/A",
        primerNombre:
          element.querySelector("#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:primerNombre")?.innerText.trim() || "N/A",
        segundoNombre:
          element.querySelector("#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:otrosNombres")?.innerText.trim() || "N/A",
        estado:
          element.querySelector("#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:estado")?.innerText.trim() || "N/A",
      };
    }
  );
}

async function processCedula(cedula, apiKey, browser) {
  const startTimeCedula = Date.now();
  const page = await browser.newPage();
  await optimizeRequestInterception(page);
  let result = null;
  try {
    await page.setViewport({ width: 1366, height: 768 });
    page.setDefaultTimeout(5000); // Timeout ligeramente aumentado para evitar falsos negativos
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    );

    // Evitar detección de webdriver
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });
      Object.defineProperty(navigator, "languages", { get: () => ["es-CO", "es", "en-US", "en"] });
      const originalQuery = window.navigator.permissions.query;
      window.navigator.permissions.query = (parameters) => {
        if (parameters.name === "notifications") {
          return Promise.resolve({ state: Notification.permission });
        }
        return originalQuery(parameters);
      };
    });

    await page.goto("https://muisca.dian.gov.co/WebRutMuisca/DefConsultaEstadoRUT.faces", {
      waitUntil: "domcontentloaded",
      timeout: 5000,
    });
    await page.waitForSelector("#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:numNit", { visible: true, timeout: 3000 });
    await page.type("#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:numNit", cedula);

    const siteKey = await page.evaluate(() => {
      const turnstileWidget = document.querySelector(".cf-turnstile");
      return turnstileWidget ? turnstileWidget.getAttribute("data-sitekey") : null;
    });

    if (siteKey) {
      console.log(`Cedula ${cedula} – Found Turnstile site key: ${siteKey}. Attempting to solve...`);
      const token = await solveTurnstileWith2Captcha(siteKey, apiKey, page.url());
      if (token) {
        console.log(`Cedula ${cedula} – Successfully obtained token. Injecting...`);
        await injectTurnstileToken(page, token);
      } else {
        console.error(`Cedula ${cedula} – Failed to solve captcha. Skipping.`);
        return { cedula, primerApellido: "ERROR", segundoApellido: "ERROR", primerNombre: "ERROR", segundoNombre: "ERROR", estado: "Error: Captcha unsolved" };
      }
    } else {
      console.warn(`Cedula ${cedula} – Turnstile site key not found. Continuing without token.`);
    }

    console.log(`Cedula ${cedula} – Clicking search button...`);
    await Promise.all([
      page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 5000 }).catch(() => console.log("Navigation timeout – continuing")),
      page.click("#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:btnBuscar"),
    ]);

    console.log(`Cedula ${cedula} – Waiting for results...`);
    const resultSelector = "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:primerApellido";
    try {
      await page.waitForSelector(resultSelector, { visible: true, timeout: 3000 });
    } catch (error) {
      console.error(`Cedula ${cedula} – No results found after waiting: ${error.message}`);
      return { cedula, primerApellido: "ERROR", segundoApellido: "ERROR", primerNombre: "ERROR", segundoNombre: "ERROR", estado: "Error: Could not find results on page" };
    }

    const data = await extractDataFromPage(page);
    if (!data) {
      console.error(`Cedula ${cedula} – Error extracting data: No data found`);
      return { cedula, primerApellido: "ERROR", segundoApellido: "ERROR", primerNombre: "ERROR", segundoNombre: "ERROR", estado: "Error: No data found" };
    }
    data.cedula = cedula;
    console.log(`Cedula ${cedula} – Successfully retrieved data.`);
    result = data;
  } catch (err) {
    console.error(`Cedula ${cedula} – Error processing document:`, err.message);
    result = { cedula, primerApellido: "ERROR", segundoApellido: "ERROR", primerNombre: "ERROR", segundoNombre: "ERROR", estado: `Error: ${err.message}` };
  } finally {
    await page.close();
    const endTimeCedula = Date.now();
    console.log(`Cedula ${cedula} – Execution time: ${endTimeCedula - startTimeCedula} ms`);
  }
  return result;
}

function limitConcurrency(tasks, limit) {
  return new Promise((resolve, reject) => {
    const results = [];
    let running = 0;
    let taskIndex = 0;
    function runTask() {
      if (taskIndex >= tasks.length && running === 0) {
        resolve(results);
        return;
      }
      while (running < limit && taskIndex < tasks.length) {
        running++;
        const currentTaskIndex = taskIndex;
        taskIndex++;
        tasks[currentTaskIndex]()
          .then((result) => {
            results[currentTaskIndex] = result;
            running--;
            runTask();
          })
          .catch((err) => {
            reject(err);
          });
      }
    }
    runTask();
  });
}

async function scrapeDianCedulas(cedulas, apiKey, concurrencyLimit = 50, globalTimeout = 10 * 60 * 1000) {
  if (!apiKey) {
    console.error("ERROR: You must provide a valid 2Captcha API key");
    return;
  }
  const balance = await checkBalance(apiKey);
  if (balance === null || balance <= 0) {
    console.error("ERROR: Your 2Captcha account has no funds or there was an error checking the balance. Please add credit before proceeding.");
    return;
  }

  const startTimeOverall = Date.now();
  console.log(`Starting scraping for ${cedulas.length} documents...`);

  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-web-security",
      "--disable-features=IsolateOrigins,site-per-process",
      "--disable-gpu",
      "--disable-dev-shm-usage",
      "--no-zygote",
      "--single-process",
    ],
  });

  try {
    const tasks = cedulas.map((cedula) => () => processCedula(cedula, apiKey, browser));
    const resultados = await Promise.race([
      limitConcurrency(tasks, concurrencyLimit),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Global timeout reached")), globalTimeout)
      ),
    ]);

    const header = "cedula,primer apellido,segundo apellido,primer nombre,segundo nombre,estado\n";
    const csvData = resultados
      .map((r) => `${r.cedula},${r.primerApellido},${r.segundoApellido},${r.primerNombre},${r.segundoNombre},${r.estado}`)
      .join("\n");
    fs.writeFileSync("dian_resultados.csv", header + csvData);
    fs.writeFileSync("dian_resultados.json", JSON.stringify(resultados, null, 2));
    console.log("Process completed. Results saved to dian_resultados.csv and dian_resultados.json");
    return resultados;
  } catch (error) {
    console.error("An error occurred during the scraping process:", error);
  } finally {
    await browser.close();
    const endTimeOverall = Date.now();
    console.log(`Total execution time: ${endTimeOverall - startTimeOverall} ms`);
  }
}

// --- PRINCIPAL ---
async function main() {
  const apiKey = "6b839fc1d6dd5a9a77261a4fdc2aeb1f";
  const excelFilePath = "/Users/alpadev/Desktop/Scrapper/js/test.xlsx";
  const concurrency = 50;
  const timeout = 10 * 60 * 1000;
  try {
    const cedulas = readCedulasFromExcel(excelFilePath);
    if (cedulas.length === 0) {
      console.error("No se encontraron cédulas en el archivo Excel.");
      return;
    }
    await scrapeDianCedulas(cedulas, apiKey, concurrency, timeout);
  } catch (error) {
    console.error("Error en la función main:", error);
  }
}

main();