import * as fs from "fs";
import { request } from "undici";

// let writeStream = fs.createWriteStream(__dirname + '/assets/merchants.csv');

type PaginationOptions = {
  page: number;
  size: number;
};

type MerchantStore = {
  name: string;
  address: string;
  id: number;
};

type MerchantListDataType = {
  address: string;
  name: string;
  storeCount: number;
  stores: MerchantStore[];
  id: number;
};

type MerchantResponseDataType = {
  total: number;
  items: MerchantListDataType[];
};

type MerchantListDataTypeWithState = Omit<
  MerchantListDataType,
  "storeCount" | "stores"
> & { state: string; city: string };

const BASE_URL =
  "https://api-site.suregifts.com/live/merchant/api/store-locations?countryId=1";

// Add these utility functions
/**
 * Splits an array into chunks of a specified size.
 *
 * @template T - The type of elements in the array.
 * @param {T[]} arr - The array to be split into chunks.
 * @param {number} size - The size of each chunk.
 * @returns {T[][]} - A new array containing the chunks.
 */
const chunk = <T>(arr: T[], size: number): T[][] => {
  return Array.from({ length: Math.ceil(arr.length / size) }, (_, i) =>
    arr.slice(i * size, i * size + size)
  );
};

/**
 * Retries a given asynchronous function a specified number of times with exponential backoff.
 *
 * @template T - The type of the value returned by the asynchronous function.
 * @param {() => Promise<T>} fn - The asynchronous function to retry.
 * @param {number} [retries=3] - The number of retry attempts. Defaults to 3.
 * @param {number} [delay=1000] - The initial delay between retries in milliseconds. Defaults to 1000ms.
 * @returns {Promise<T>} - A promise that resolves to the result of the asynchronous function, or rejects if all retries fail.
 * @throws {Error} - Throws the error from the asynchronous function if all retries fail.
 */
const retry = async <T>(
  fn: () => Promise<T>,
  retries = 3,
  delay = 1000
): Promise<T> => {
  try {
    return await fn();
  } catch (error) {
    if (retries === 0) throw error;
    await sleep(delay);
    return retry(fn, retries - 1, delay * 2);
  }
};


/**
 * Fetches merchant locations with pagination and processes them in batches.
 *
 * @param pagination - The pagination options including page number and size.
 * @param continuousRequest - If true, fetches all pages continuously until no more items are found. Defaults to true.
 * @throws Will throw an error if the page or size is less than or equal to zero.
 * @returns A promise that resolves to an array of processed merchant store data with state and city information.
 */
async function fetchMerchantsLocations(
  pagination: PaginationOptions,
  continuousRequest: boolean = true
) {
  if (pagination.size <= 0 || pagination.page <= 0) {
    throw Error("page or size can not be equal to or less than zero");
  }

  let currentPage = pagination.page;
  let allStores: MerchantStore[] = [];

  if (continuousRequest) {
    while (true) {
      console.log(`Fetching page ${currentPage}...`);

      try {
        const response = await retry(() =>
          request(`${BASE_URL}&page=${currentPage}&size=${pagination.size}`, {
            method: "GET",
            headers: {
              "Content-Type": "application/json",
              "Access-Control-Allow-Origin": "*",
            },
          })
        );

        const responseData =
          (await response.body.json()) as MerchantResponseDataType;

        if (!responseData.items?.length) {
          console.log("No more items to fetch");
          break;
        }

        /**
         * Filter out stores with missing or invalid data
         */
        const pageStores = responseData.items
          .map((merchant) => merchant.stores)
          .flat()
          .filter((store): store is MerchantStore => !!store);

        // Append stores to the allStores array
        allStores.push(...pageStores);
        console.log(
          `Retrieved ${pageStores.length} stores from page ${currentPage}`
        );

        // Rate limiting
        await sleep(2000);
        currentPage++;
      } catch (error) {
        console.error(`Failed to fetch page ${currentPage}:`, error);
        break;
      }
    }
  } else {
    // Single page fetch
    const response = await retry(() =>
      request(`${BASE_URL}&page=${pagination.page}&size=${pagination.size}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      })
    );

    const responseData =
      (await response.body.json()) as MerchantResponseDataType;
    allStores = responseData.items
      .map((merchant) => merchant.stores)
      .flat()
      .filter((store): store is MerchantStore => !!store);
  }

  console.log(`::: Total stores to process: ${allStores.length} :::`);

  // Process in batches of 5
  const batches = chunk(allStores, 15);
  const results: MerchantListDataTypeWithState[] = [];

  for (const [index, batch] of batches.entries()) {
    console.log(`Processing batch ${index + 1}/${batches.length}`);

    const batchResults = await Promise.all(
      batch.map(async (store) => {
        try {
          const { state, city = "" } = await retry(() =>
            GetStateProvider(store.address, "OPEN_CAGE")
          );

          return {
            ...store,
            state,
            city,
          };
        } catch (error) {
          console.error(`Failed to process store: ${store.id}`, error);
          return null;
        }
      })
    );

    const validResults = batchResults.filter(
      (result): result is MerchantListDataTypeWithState => result !== null
    );

    results.push(...validResults);

    if (index < batches.length - 1) {
      await sleep(1000);
    }
  }

  console.log(`Successfully processed ${results.length} stores`);
  writeToCsv(results);
  return results;
}

async function parseMerchants(merchants: MerchantListDataType) {
  try {
  } catch (error) {}
}

// Update GetStateProvider to include rate limiting
function GetStateProvider(
  address: string,
  provider: "OPEN_CAGE" | "NOMINATIM" = "OPEN_CAGE"
) {
  return retry(() => {
    if (provider === "OPEN_CAGE") {
      return getStateFromAddressOpenCage(address);
    } else {
      return getStateFromAddressNominatim(address);
    }
  });
}

const getStateFromAddressOpenCage = async (address: string) => {
  const apiKey = process.env.OPEN_CAGE_API_KEY;
  const url = `https://api.opencagedata.com/geocode/v1/json?q=${encodeURIComponent(
    address
  )}&key=${apiKey}`;

  const response = await request(url, {
    method: "GET",
  });
  const results = ((await response.body.json()) as { results: any[] }).results;

  if (results.length > 0) {
    console.log(
      `::: components ${JSON.stringify(results[0].components, null, 2)} :::`
    );
    const state = results[0].components.state;
    const city =
      results[0].components?._normalized_city ??
      results[0].components?.town ??
      "";
    return { state, city };
  }
  return { state: "", city: "" };
};

const getStateFromAddressNominatim = async (address: string) => {
  // sleep for 1 second
  sleep(1000);
  const encoded = new URLSearchParams(`q=${address}`).toString().slice(2);
  const api_key = process.env.NOMINATIM_API_KEY;
  const url = `https://api.geocodify.com/v2/geocode?api_key=${api_key}&q=${encoded}`;

  const response = await request(url, {
    method: "GET",
    headers: {
      "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
      Pragma: "no-cache",
      Expires: "0",
    },
  });
  const results = (await response.body.json()) as any;
  console.log(
    `::: nominatim results => ${JSON.stringify(results)} \n url => ${url} :::`
  );

  if (Object.keys(results).length > 0) {
    const properties = results.response?.features?.[0]?.properties;
    const state = properties?.region ?? "";
    // .split(",")
    // .find((part) => part.trim().includes("State"));
    return state;
  }
  return null;
};

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const writeToCsv = (
  merchants: MerchantListDataTypeWithState[],
  overwrite: boolean = false
) => {
  /**
   * 1. create a write stream to csv file
   * 2. create the headers with the first row, by using the properties of the first merchant
   * 3. then write each merchant to the csv file, with each merchant exisiting once by their merchant "id"
   *    (A) the id already exists in the file at the time of writing , do not overwrite the merchant
   *    (B) also append merchants to file and do not overwrite merchants that already exist in the file
   */
  if (!fs.existsSync("assets")) {
    fs.mkdirSync("assets");
  }

  const csvPath = "assets/merchants.csv";

  // rather loop through all the merchants and get all unique properties
  const headers = Array.from(
    new Set<string>(merchants.map((merchant) => Object.keys(merchant)).flat())
  );

  console.log(`::: headers => ${JSON.stringify(headers, null, 2)} :::`);

  // const headers = Object.keys(merchants[0]);
  // Check if file exists and validate headers
  if (fs.existsSync(csvPath)) {
    const existingContent = fs.readFileSync(csvPath, "utf-8");
    const existingLines = existingContent
      .split("\n")
      .filter((line) => line.trim());

    if (existingLines.length > 0) {
      const existingHeaders = existingLines[0].split(",");
      if (!headers.every((h) => existingHeaders.includes(h))) {
        throw new Error(
          "CSV headers mismatch! Existing headers do not match new data structure."
        );
      }

      // Get existing merchant IDs
      const existingIds = new Set(
        existingLines.slice(1).map((line) => {
          const values = line.split(",");
          const idIndex = existingHeaders.indexOf("id");
          return parseInt(values[idIndex]);
        })
      );

      // Filter out merchants that already exist (unless overwriting)
      merchants = merchants.filter((merchant) => {
        if (overwrite) return true;
        return !existingIds.has(merchant.id);
      });
    }
  } else {
    // Create new file with headers
    fs.writeFileSync(csvPath, headers.join(",") + "\n");
  }

  // Append new merchants
  const writeStream = fs.createWriteStream(csvPath, { flags: "a" });

  merchants.forEach((merchant) => {
    const values = headers.map((header) => {
      const value = merchant[header as keyof MerchantListDataType];
      return typeof value === "string" ? `"${value}"` : value;
    });
    writeStream.write(values.join(",") + "\n");
  });

  writeStream.end();
};

function capitalize(input: string | string[]): string[] {
  // Helper function to capitalize single word
  const capitalizeWord = (word: string): string => {
    if (!word) return word;
    return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
  };

  // Handle array input
  if (Array.isArray(input)) {
    return input.map(capitalizeWord);
  }

  // Handle single string with spaces
  if (input.includes(" ")) {
    return [input.split(" ").map(capitalizeWord).join(" ")];
  }

  return [capitalizeWord(input)];
}

(async () =>
  await fetchMerchantsLocations({
    page: 1,
    size: 6,
  }))();
