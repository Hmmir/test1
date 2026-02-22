/**
 * @param { Object } params - параметры
 * @param { string } params.url - часть url после корневой api
 * @param { Object } params.payload - часть url после корневой api
 * @param { string } [params.method] - по умолчание - post
 * @param { strting } [host]  - адрес хоста для тестирования через localtunnel
 * @returns{ UrlFetchApp.HTTPResponse }
 */
function getApiBaseUrl() {
  const value = PropertiesService.getScriptProperties().getProperty("BTLZ_API_BASE_URL");
  const normalized = value ? String(value).trim() : "";
  if (normalized) return normalized;
  throw new Error("BTLZ_API_BASE_URL is not configured. Set your backend URL first.");
}

function setApiBaseUrl(url) {
  if (!url || typeof url !== "string") throw new Error("url must be a non-empty string");
  PropertiesService.getScriptProperties().setProperty("BTLZ_API_BASE_URL", url.trim());
  return getApiBaseUrl();
}

function joinUrl(base, path) {
  const left = String(base || "").replace(/\/$/, "");
  const right = String(path || "").replace(/^\//, "");
  return `${left}/${right}`;
}

function fetch({ url, payload, method = 'post', token }, host = undefined) {
  const baseUrl = host || getApiBaseUrl();
  const options = {
    method,
    'contentType': 'application/json',
    'muteHttpExceptions': true,
    'headers': {},
  }
  if (payload) options.payload = JSON.stringify(payload)
  if (token)
    options["headers"]['Authorization'] = `Bearer ${token}`;
  const response = UrlFetchApp.fetch(joinUrl(baseUrl, url), options);
  return response;
}

// /**
//  * Показать окно управления токенами
//  * @param { string } ssId - id обращающейся таблицы
//  * @param { string } title - заголовок, по умолчанию 'Токены WB'
//  * @param { string } ssId - id обращающейся таблицы
//  */
// function showWbTokensModal({ ssId, width = 1346, height = 750, title = 'Токены WB' }) {
//   let htmlOutput = HtmlService.createHtmlOutput(`<iframe src="https://btlz-api.ru/mp/wb/tokens/?ssId=${ssId}" width="100%" style="border: 0px solid red; height: ${height - 50}px; min-height: 11rem"></iframe>`)
//   htmlOutput.setWidth(width)
//   htmlOutput.setHeight(height)
//   SpreadsheetApp.getUi().showModalDialog(htmlOutput, title);
// }

/**
 * @param { UrlFetchApp.HTTPResponse } response
 */
function handleResponse(response) {
  const contentText = response.getContentText();
  const data = JSON.parse(contentText);
  return data
}


function admin(token) {
  function unsupportedLegacyRoute(name) {
    throw new Error(`[BtlzApi] '${name}' is not supported by backend_compat API. Use admin(token).backendCompat.* methods.`);
  }

  return {
    user: () => ({
      read: () => unsupportedLegacyRoute("admin.user.read"),
      update: () => unsupportedLegacyRoute("admin.user.update"),
      spreadsheets: () => ({
        read: () => unsupportedLegacyRoute("admin.user.spreadsheets.read"),
        create: () => unsupportedLegacyRoute("admin.user.spreadsheets.create"),
      }),
      spreadsheet: () => ({
        read: () => unsupportedLegacyRoute("admin.user.spreadsheet.read"),
        update: () => unsupportedLegacyRoute("admin.user.spreadsheet.update"),
        delete: () => unsupportedLegacyRoute("admin.user.spreadsheet.delete"),
        dataset: {
          read: () => unsupportedLegacyRoute("admin.user.spreadsheet.dataset.read"),
          create: () => unsupportedLegacyRoute("admin.user.spreadsheet.dataset.create"),
          update: () => unsupportedLegacyRoute("admin.user.spreadsheet.dataset.update"),
          delete: () => unsupportedLegacyRoute("admin.user.spreadsheet.dataset.delete"),
        },
        datasets: {
          read: () => unsupportedLegacyRoute("admin.user.spreadsheet.datasets.read"),
        },
      }),
      wb: { token: {
        read: () => unsupportedLegacyRoute("admin.user.wb.token.read"),
        create: () => unsupportedLegacyRoute("admin.user.wb.token.create"),
        delete: () => unsupportedLegacyRoute("admin.user.wb.token.delete"),
      } },
      ozon: { apikey: {
        read: () => unsupportedLegacyRoute("admin.user.ozon.apikey.read"),
        create: () => unsupportedLegacyRoute("admin.user.ozon.apikey.create"),
        delete: () => unsupportedLegacyRoute("admin.user.ozon.apikey.delete"),
      } },
    }),
    users: {
      read: () => unsupportedLegacyRoute("admin.users.read"),
      create: () => unsupportedLegacyRoute("admin.users.create"),
    },
    backendCompat: {
      registerSpreadsheet: ({ spreadsheet_id, owner_email = undefined }) =>
        handleResponse(fetch({ token, method: "post", url: "/admin/spreadsheets/register", payload: { spreadsheet_id, owner_email } })),

      addWbToken: ({ spreadsheet_id, wb_token, owner_email = undefined }) =>
        handleResponse(fetch({ token, method: "post", url: "/admin/wb/tokens/add", payload: { spreadsheet_id, token: wb_token, owner_email } })),

      listWbTokens: ({ spreadsheet_id }) =>
        handleResponse(fetch({ token, method: "post", url: "/admin/wb/tokens/list", payload: { spreadsheet_id } })),

      getWbToken: ({ spreadsheet_id }) =>
        handleResponse(fetch({ token, method: "post", url: "/ss/wb/token/get", payload: { spreadsheet_id } })),

      datasetsUpdate: ({ ssId }) =>
        handleResponse(fetch({ token, method: "post", url: "/ss/datasets/update", payload: { ssId } })),

      actions: ({ action, payload = {} }) =>
        handleResponse(fetch({ token, method: "post", url: "/actions", payload: { action, ...payload } })),
    },
  }
}
/**
 * @typedef { Object } ssItem
 * @property { number } client_id
 * @property { string } spreadsheet_id
 * @property { Date } updated_at
 * @property { boolean } is_active
 * @property { Object|null } params.update_results
 */
/**
 * @typedef { Object } userItem
 * @property { number } id
 * @property { string } email
 * @property { string } name
 * @property { string } password
 * @property { roles } roles
 * @property { boolean } is_active
 * @property { string } activation_link
 * @property { Date } created_at
 * @property { string[] } spreadsheets
 */
/**
 * @typedef { Object } ssFullItem
 * @property { number } client_id
 * @property { string } spreadsheet_id
 * @property { Date } updated_at
 * @property { boolean } is_active
 * @property { Object|null } params.update_results
 * @property { import("#ssDatasets/ssDatasets.types.js").spreadsheetDatasetItem[] } datasets
 */
/**
 * @typedef { Object } ssItem
 * @property { number } client_id
 * @property { string } spreadsheet_id
 * @property { Date } updated_at
 * @property { boolean } is_active
 * @property { Object|null } params.update_results
 */
/**
 * @typedef { Object } wbTokenItem
 * @property { string } token
 * @property { string } description
 * @property { Date } exp
 * @property { string } sid
 * @property { number } [iid]
 * @property { number } [uid]
 * @property { boolean } content
 * @property { boolean } analytics
 * @property { boolean } prices
 * @property { boolean } marketplace
 * @property { boolean } statistics
 * @property { boolean } adverts
 * @property { boolean } questions
 * @property { boolean } recommendations
 * @property { boolean } read_only
 */
/**
 * @typedef { Object } ozonApiKeyItem
 * @property { number } seller_client_id
 * @property { string } seller_api_key
 * @property { string } performance_client_id
 * @property { string } performance_client_secret
 * @property { string } name
 */
