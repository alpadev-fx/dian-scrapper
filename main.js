import puppeteer from "puppeteer";
import fs from "fs";
import PDFDocument from "pdfkit";
import axios from "axios";

/**
 * Checks the balance of a 2Captcha account
 * @param {string} apiKey - Your 2Captcha API key
 */
async function checkBalance(apiKey) {
  try {
    const response = await axios.get("https://2captcha.com/res.php", {
      params: { key: apiKey, action: "getbalance", json: 1 },
    });
    console.log("Account balance:", response.data);
    return response.data.request;
  } catch (error) {
    console.error("Error checking balance:", error.message);
    return null;
  }
}

/**
 * Scrapes DIAN RUT information with automated Cloudflare Turnstile captcha solving
 * @param {string[]} cedulas - Array of document numbers to query
 * @param {string} apiKey - Your 2Captcha API key
 * @param {boolean} takeScreenshots - Whether to take diagnostic screenshots
 */
async function scrapeDianCedulas(
  cedulas = ["1047473418"],
  apiKey,
  takeScreenshots = true
) {
  if (!apiKey || apiKey === "YOUR_2CAPTCHA_API_KEY") {
    console.error("ERROR: You must provide a valid 2Captcha API key");
    return;
  }

  const balance = await checkBalance(apiKey);
  if (balance === "0") {
    console.error(
      "ERROR: Your 2Captcha account has no funds. Please add credit to your account before proceeding."
    );
    return;
  }

  console.log(
    `Starting scraping process for ${cedulas.length} document numbers...`
  );

  if (takeScreenshots) {
    if (!fs.existsSync("./screenshots")) {
      fs.mkdirSync("./screenshots");
    }
  }

  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-web-security",
      "--disable-features=IsolateOrigins,site-per-process",
    ],
  });

  const resultados = [];
  const page = await browser.newPage();

  await page.setViewport({ width: 1366, height: 768 });

  page.setDefaultTimeout(30000);

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
  );

  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", {
      get: () => false,
    });

    Object.defineProperty(navigator, "languages", {
      get: () => ["es-CO", "es", "en-US", "en"],
    });

    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => {
      if (parameters.name === "notifications") {
        return Promise.resolve({ state: Notification.permission });
      }
      return originalQuery(parameters);
    };
  });

  try {
    for (let i = 0; i < cedulas.length; i++) {
      const cedula = cedulas[i];
      console.log(`Processing document ${i + 1}/${cedulas.length}: ${cedula}`);

      try {
        await page.goto(
          "https://muisca.dian.gov.co/WebRutMuisca/DefConsultaEstadoRUT.faces",
          {
            waitUntil: "networkidle2",
            timeout: 30000,
          }
        );

        if (takeScreenshots) {
          await page.screenshot({
            path: `./screenshots/${cedula}_1_initial_page.png`,
          });
        }

        await page.waitForSelector(
          "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:numNit",
          { visible: true }
        );

        await page.type(
          "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:numNit",
          cedula
        );

        if (takeScreenshots) {
          await page.screenshot({
            path: `./screenshots/${cedula}_2_filled_form.png`,
          });
        }

        const siteKey = await page.evaluate(() => {
          const turnstileWidget = document.querySelector(".cf-turnstile");
          return turnstileWidget
            ? turnstileWidget.getAttribute("data-sitekey")
            : null;
        });

        if (!siteKey) {
          console.warn(
            "Could not find Turnstile site key on page. Attempting to proceed anyway."
          );
        } else {
          console.log(
            `Found Turnstile site key: ${siteKey}. Attempting to solve...`
          );

          const token = await solveTurnstileWith2Captcha(
            siteKey,
            apiKey,
            page.url()
          );

          if (token) {
            console.log(
              "Successfully obtained Turnstile token. Injecting into page..."
            );

            await page.evaluate((token) => {
              const turnstileFields = Array.from(
                document.querySelectorAll(
                  'input[name*="cf-turnstile"], input[name*="turnstile"], input[name*="captcha"]'
                )
              );
              if (turnstileFields.length > 0) {
                turnstileFields.forEach((field) => (field.value = token));
                console.log(
                  "Injected token into existing fields:",
                  turnstileFields.length
                );
              }

              const forms = document.querySelectorAll("form");
              forms.forEach((form) => {
                let tokenInput = form.querySelector(
                  '[name="cf-turnstile-response"]'
                );
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

              const turnstileElements =
                document.querySelectorAll(".cf-turnstile");
              turnstileElements.forEach((el) => {
                el.setAttribute("data-token", token);
              });

              return {
                forms: forms.length,
                turnstileElements: turnstileElements.length,
              };
            }, token);

            if (takeScreenshots) {
              await page.screenshot({
                path: `./screenshots/${cedula}_3_token_injected.png`,
              });
            }
          } else {
            console.error(
              `Failed to solve captcha for document ${cedula}. Skipping.`
            );
            continue;
          }
        }

        console.log("Clicking search button...");

        try {
          await Promise.all([
            page
              .waitForNavigation({ timeout: 10000 })
              .catch(() =>
                console.log("Navigation timeout - continuing anyway")
              ),
            page.click(
              "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:btnBuscar"
            ),
          ]);

          if (takeScreenshots) {
            await page.screenshot({
              path: `./screenshots/${cedula}_4_after_button_click.png`,
            });
          }

          await new Promise((resolve) => setTimeout(resolve, 5000));

          if (takeScreenshots) {
            await page.screenshot({
              path: `./screenshots/${cedula}_5_after_waiting.png`,
            });
          }

          console.log("Waiting for results...");

          const resultSelectors = [
            "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:primerApellido",
            "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:estado",
            ".informacionEstadoRut",
          ];

          let resultsFound = false;

          for (const selector of resultSelectors) {
            try {
              await page.waitForSelector(selector, {
                visible: true,
                timeout: 10000,
              });
              console.log(`Found results with selector: ${selector}`);
              resultsFound = true;
              break;
            } catch (e) {
              console.log(`Selector ${selector} not found, trying next...`);
            }
          }

          if (!resultsFound) {
            const errorMessage = await page.evaluate(() => {
              const errorElements = document.querySelectorAll(
                ".ui-messages-error, .error-message, .alert-error"
              );
              return Array.from(errorElements)
                .map((el) => el.innerText)
                .join(" | ");
            });

            if (errorMessage) {
              console.error(`Form submission error: ${errorMessage}`);
            }

            if (takeScreenshots) {
              await page.screenshot({
                path: `./screenshots/${cedula}_6_no_results_found.png`,
              });
            }

            const html = await page.content();
            fs.writeFileSync(`./screenshots/${cedula}_debug_html.html`, html);

            console.error(`No results elements found for document ${cedula}`);

            resultados.push({
              cedula: cedula,
              primerApellido: "ERROR",
              segundoApellido: "ERROR",
              primerNombre: "ERROR",
              segundoNombre: "ERROR",
              estado: "Error: Could not find results on page",
            });

            continue;
          }

          const data = await page.evaluate(() => {
            try {
              return {
                primerApellido:
                  document
                    .querySelector(
                      "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:primerApellido"
                    )
                    ?.innerText.trim() || "N/A",
                segundoApellido:
                  document
                    .querySelector(
                      "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:segundoApellido"
                    )
                    ?.innerText.trim() || "N/A",
                primerNombre:
                  document
                    .querySelector(
                      "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:primerNombre"
                    )
                    ?.innerText.trim() || "N/A",
                segundoNombre:
                  document
                    .querySelector(
                      "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:otrosNombres"
                    )
                    ?.innerText.trim() || "N/A",
                estado:
                  document
                    .querySelector(
                      "#vistaConsultaEstadoRUT\\:formConsultaEstadoRUT\\:estado"
                    )
                    ?.innerText.trim() || "N/A",
              };
            } catch (err) {
              return {
                error: `Error extracting data: ${err.message}`,
                html: document.body.innerHTML.substring(0, 500) + "...", // First 500 chars of HTML for debugging
              };
            }
          });

          if (data.error) {
            console.error(`Error extracting data: ${data.error}`);
            if (takeScreenshots) {
              await page.screenshot({
                path: `./screenshots/${cedula}_7_data_extraction_error.png`,
              });
            }
            resultados.push({
              cedula: cedula,
              primerApellido: "ERROR",
              segundoApellido: "ERROR",
              primerNombre: "ERROR",
              segundoNombre: "ERROR",
              estado: `Error: ${data.error}`,
            });
          } else {
            data.cedula = cedula;
            resultados.push(data);
            console.log(
              `Successfully retrieved data for document ${cedula}:`,
              data
            );

            if (takeScreenshots) {
              await page.screenshot({
                path: `./screenshots/${cedula}_7_success.png`,
              });
            }
          }
        } catch (navError) {
          console.error(
            `Navigation error for document ${cedula}:`,
            navError.message
          );

          if (takeScreenshots) {
            await page.screenshot({
              path: `./screenshots/${cedula}_error_${navError.message.substring(
                0,
                20
              )}.png`,
            });
          }

          resultados.push({
            cedula: cedula,
            primerApellido: "ERROR",
            segundoApellido: "ERROR",
            primerNombre: "ERROR",
            segundoNombre: "ERROR",
            estado: `Error: Navigation failed - ${navError.message}`,
          });
        }

        const delay = Math.floor(Math.random() * 5000) + 5000;
        console.log(`Waiting ${delay}ms before next request...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      } catch (err) {
        console.error(`Error processing document ${cedula}:`, err.message);

        if (takeScreenshots) {
          try {
            await page.screenshot({
              path: `./screenshots/${cedula}_error_general.png`,
            });
          } catch (screenshotErr) {
            console.error(
              "Could not take error screenshot:",
              screenshotErr.message
            );
          }
        }

        resultados.push({
          cedula: cedula,
          primerApellido: "ERROR",
          segundoApellido: "ERROR",
          primerNombre: "ERROR",
          segundoNombre: "ERROR",
          estado: `Error: ${err.message}`,
        });
      }
    }
  } catch (err) {
    console.error("Fatal error:", err);
  } finally {
    await generatePDF(resultados);

    fs.writeFileSync(
      "dian_resultados.json",
      JSON.stringify(resultados, null, 2)
    );

    await browser.close();

    console.log(
      `Process completed. Results saved to dian_resultados.pdf and dian_resultados.json`
    );
  }

  return resultados;
}

/**
 * Solves Cloudflare Turnstile captcha using 2Captcha service
 * @param {string} siteKey - Cloudflare Turnstile site key from the page
 * @param {string} apiKey - Your 2Captcha API key
 * @param {string} pageUrl - URL of the page with the captcha
 * @returns {Promise<string|null>} - Captcha token or null if solving failed
 */
async function solveTurnstileWith2Captcha(siteKey, apiKey, pageUrl) {
  try {
    console.log("Submitting Turnstile captcha to 2Captcha...");

    const submitResponse = await axios.get("https://2captcha.com/in.php", {
      params: {
        key: apiKey,
        method: "turnstile",
        sitekey: siteKey,
        pageurl: pageUrl,
        json: 1,
      },
      timeout: 30000,
    });

    if (!submitResponse.data.status || submitResponse.data.status !== 1) {
      throw new Error(
        `2Captcha submission error: ${submitResponse.data.request}`
      );
    }

    const captchaId = submitResponse.data.request;
    console.log(`Captcha submitted successfully. ID: ${captchaId}`);

    let token = null;
    let attempts = 0;
    const maxAttempts = 30;

    while (attempts < maxAttempts) {
      attempts++;

      await new Promise((resolve) => setTimeout(resolve, 5000));

      console.log(
        `Checking captcha solution (attempt ${attempts}/${maxAttempts})...`
      );

      try {
        const resultResponse = await axios.get("https://2captcha.com/res.php", {
          params: {
            key: apiKey,
            action: "get",
            id: captchaId,
            json: 1,
          },
          timeout: 10000,
        });

        if (resultResponse.data.status === 1) {
          token = resultResponse.data.request;
          console.log("Captcha solved successfully!");
          break;
        } else if (resultResponse.data.request !== "CAPCHA_NOT_READY") {
          throw new Error(`2Captcha error: ${resultResponse.data.request}`);
        }
      } catch (axiosError) {
        console.error(
          `Error checking captcha solution (attempt ${attempts}):`,
          axiosError.message
        );
      }
    }

    if (!token) {
      throw new Error("Captcha solving timed out");
    }

    return token;
  } catch (error) {
    console.error("Error solving captcha with 2Captcha:", error.message);
    return null;
  }
}

/**
 * Generates a PDF report with the results
 * @param {Array} resultados - Array of query results
 */
async function generatePDF(resultados) {
  const doc = new PDFDocument();
  const pdfStream = fs.createWriteStream("dian_resultados.pdf");
  doc.pipe(pdfStream);

  doc.fontSize(18).text("DIAN RUT Query Results", { align: "center" });
  doc.moveDown();
  doc
    .fontSize(10)
    .text(`Generated on: ${new Date().toLocaleString()}`, { align: "center" });
  doc.moveDown(2);

  resultados.forEach((res, index) => {
    doc
      .fontSize(14)
      .text(`Consulta ${index + 1}: ${res.cedula}`, { underline: true });
    doc.fontSize(12).text(`Primer Apellido: ${res.primerApellido}`);
    doc.text(`Segundo Apellido: ${res.segundoApellido}`);
    doc.text(`Primer Nombre: ${res.primerNombre}`);
    if (res.segundoNombre) {
      doc.text(`Segundo Nombre: ${res.segundoNombre}`);
    }
    doc.text(`Estado: ${res.estado}`);
    doc.moveDown(2);

    if (index < resultados.length - 1) {
      doc.moveTo(50, doc.y).lineTo(550, doc.y).stroke();
      doc.moveDown();
    }
  });

  doc.moveDown();
  doc.fontSize(14).text("Summary", { underline: true });
  doc.fontSize(12).text(`Total documents processed: ${resultados.length}`);

  const successCount = resultados.filter(
    (r) => r.estado !== "ERROR" && !r.estado.startsWith("Error")
  ).length;
  doc.text(`Successful queries: ${successCount}`);
  doc.text(`Failed queries: ${resultados.length - successCount}`);

  doc.end();

  return new Promise((resolve, reject) => {
    pdfStream.on("finish", resolve);
    pdfStream.on("error", reject);
  });
}

export { scrapeDianCedulas, checkBalance, solveTurnstileWith2Captcha };

scrapeDianCedulas(
  ["1047473418", "73089347", "42207035", "1047447085"],
  "6b839fc1d6dd5a9a77261a4fdc2aeb1f",
  false
);
