/*//@ts-check*/
/*import { DB } from "../core/db/db"*/
/*import { PartitionedDB } from "../core/partitionedDB/partitionedDb"*/
/*import { Registry } from "../core/registry/registry"*/
/*import { config } from "./config.js"*/

class Registry {

  /** Хранит основной ss 
   * при первом вызове,без предварительной установки ssId методом setSsId 
   * устанавливает значения getActiveSpreadsheet(), для ss и ssId
   * @returns { SpreadsheetApp.Spreadsheet } */
  static get ss() {
    return Registry._initOnce("ss", () => {
      //@ts-ignore
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const ssId = Registry._initOnce("ssId", ss.getId());
      Registry.ssMap.set(ssId, ss);
      return ss
    })
  }

  /** Возвращает ssId основной ss 
   * при первом вызове, без предварительной установки ssId методом setSsId
   * устанавливает значения getActiveSpreadsheet(), для ss и ssId
   * @returns { string } */
  static get ssId() {
    //@ts-ignore
    const ss = Registry._initOnce("ss", SpreadsheetApp.getActiveSpreadsheet())
    const ssId = Registry._initOnce("ssId", ss.getId())
    Registry.ssMap.set(ssId, ss);
    return ssId
  }

  /**Устанавливает ssId для ss без возможности дальнейшего изменения
   * Необходимо установить до первого вызова обращения к ss
   * Иначе в ssId будет установлено SpreadsheetApp.getActiveSpreadsheet().getId()
   * @param { string } ssId
   * @returns { string }
   */
  static setSsId(ssId) {
    //@ts-ignore
    delete Registry["ss"];
    Object.defineProperty(Registry, "ss", {
      configurable: true,
      get() { return Registry._initOnce("ss", () => Registry.getSs(Registry.ssId)) }
    })
    return Registry._initOnce("ssId", ssId)
  }

  /** Получает и сохраняет ss в ssMap по ssId
   * @param { string } ssId
   * 
   * @returns { SpreadsheetApp.Spreadsheet } */
  static getSs(ssId) {
    if (!Registry.ssMap.has(ssId)) {
      const ss = SpreadsheetApp.openById(ssId)
      Registry.ssMap.set(ssId, ss);
    }
    const result = Registry.ssMap.get(ssId);
    if (!result) throw new Error(`Invalid ssId[${ssId}]`)
    return result;
  }

  /** @returns { Map<string, SpreadsheetApp.Spreadsheet>  } */
  static get ssMap() {
    return Registry._initOnce("ssMap", () => new Map())
  }

  /** Получает и сохраняет ss в sheetsMap по ssId и sheetName
   * @param { string } sheetName
   * @param { string } [ssId]
   * @returns { SpreadsheetApp.Sheet } */
  static getSheet(sheetName, ssId = undefined) {
    if (!ssId) ssId = Registry.ssId
    const hash = ssId + "." + sheetName;

    if (!Registry.sheetsMap.has(hash) || Registry.sheetsMap.get(hash) === null) {
      const sheet = Registry.getSs(ssId).getSheetByName(sheetName)
      if (!sheet) throw new Error(`Sheet[${hash}] is invalid`)
      Registry.sheetsMap.set(hash, sheet);
    }
    const result = Registry.sheetsMap.get(hash);
    if (!result) throw new Error(`Sheet[${hash}] is not defined`)
    return result
  }

  /** @returns { Map<string, SpreadsheetApp.Sheet> } */
  static get sheetsMap() {
    return Registry._initOnce("sheetsMap", () => new Map())
  }

  /** Устанавливает timestamp начала выполнения скрипта */
  static set startTime(timestamp) {
    //@ts-ignore
    return Registry._initOnce("startTime", timestamp)
  }

  /** Возвращает timestamp начала выполнения скрипта,
   * устанавливает текущий timestamp если не был установлен ранее */
  static get startTime() {
    return Registry._initOnce("startTime", Date.now())
  }

  /** Возвращает значение безопасное время выполнения скрипта в секундах
   * если не было установлено ранее устанавливает значение по умолчанию - 300 сек
   * @returns {Number} - seconds */
  static get safeExecutionTime() {
    return Registry._initOnce("safeExecutionTime", 300);
  }

  /** Возвращает значение безопасное время выполнения скрипта в секундах
   * если не было установлено ранее устанавливает значение по умолчанию - 300 сек
   * @returns {Number} - seconds */
  static get executionTime() {
    return (Date.now() - Registry.startTime) / 1000;
  }

  /** Возвращает значение безопасное время выполнения скрипта в секундах
   * если не было установлено ранее устанавливает значение по умолчанию - 300 сек
   * @returns {Number} - seconds */

  static set safeExecutionTime(seconds) {
    //@ts-ignore
    return Registry._initOnce("safeExecutionTime", seconds);
  }

  /** Возвращает сообщение об истечении времени безопасного выполнения скрипта
   * используется для проверок
   * @returns {String} - "safe time expired" */
  static get timeExpirationErrorMessage() {
    return Registry._initOnce("timeExpirationErrorMessage", "safe time expired");
  }

  /** Функция для проверки безопасного времени выполнения скрипта
   * выбрасывает исключение с сообщением timeExpirationErrorMessage
   * @param { boolean } [throwError=true] - флаг выброса ошибки. По умолчанию true
   * @returns { boolean }
   * @throws { Error }
   */
  static checkExecutionTime(throwError = true) {
    if (throwError && Registry.executionTime > Registry.safeExecutionTime)
      throw new Error(Registry.timeExpirationErrorMessage);
    return Registry.executionTime > Registry.safeExecutionTime
  }
  /** Массив для хранения ошибок 
   * @returns {String[]} */
  static get errors() {
    return Registry._initOnce("errors", [])
  }

  /** Метод добавляющий строку в массив errors
   * @param { string } message
   * @returns { string[] } */
  static appendError(message) {
    Registry.errors.push(message);
    return Registry.errors
  }

  /** Флаг глобально отключающий функции timeStart, timeEnd*/
  static set timeLog(boolean) {
    // @ts-ignore
    return Registry._initOnce("timeLog", boolean);
  }

  /** Флаг глобально отключающий функции timeStart, timeEnd*/
  static get timeLog() { return Registry._initOnce("timeLog", false); }

  /** Флаг (общего назначения)*/
  static set debugMode(boolean) {
    //@ts-ignore
    return Registry._initOnce("debugMode", boolean);
  }

  /** Флаг (общего назначения)*/
  static get debugMode() { return Registry._initOnce("debugMode", false); }

  /** Флаг (общего назначения)*/
  static set dryRun(boolean) {
    //@ts-ignore
    return Registry._initOnce("dryRun", boolean);
  }

  /** Флаг (общего назначения)*/
  static get dryRun() { return Registry._initOnce("dryRun", false); }

  /** Инициализирует свойства класса
   * @param { string } key - ключ по которому происходит обращение, property_name.
   * @param { Function | any } valueOrCallback - значение или callback возвращающий значение для инициализации;
   * @returns { any } - значение valueOrCallback;
   */
  static _isInit(key, valueOrCallback) {
    if (this["_" + key] === undefined) {
      this["_" + key] = typeof valueOrCallback == "function" ? valueOrCallback() : valueOrCallback
    }
    return this["_" + key];
  }

  /** Инициализирует свойства класса и заменяет геттер свойством, блокируя перезапись.
   * @param { string } key - ключ по которому происходит обращение, property_name.
   * @param { Function | any } valueOrCallback - значение или callback возвращающий значение для инициализации;
   * @returns { any } - значение valueOrCallback;
   */
  static _initOnce(key, valueOrCallback) {
    delete this[key];
    Object.defineProperty(this, key, {
      configurable: false,
      writable: false,
      value: typeof valueOrCallback == "function" ? valueOrCallback() : valueOrCallback
    })
    return this[key]
  }

  /** Сбрасывает свойство класса.
   * Используется например для сброса range после операций вставки.
   * @param { string } key - ключ по которому происходит обращение, property_name.
   */
  static reset(key) {
    this["_" + key] = undefined;
  }

}
Registry.startTime = Date.now();

/**Дополнительный класс 
 * для объявления сущностей используемых в конкретном приложении
 * для удобства использования имеет имя $
 */
class $ extends Registry {
  static get hello() { return "hello" }
  static get key() {
    try {
      throw new Error()
    } catch (e) {
      return e.stack.match(/(?:(?:get )(?:key)(?:.|\n)*?)?(?:get )(?<key>\b.+?\b)/m).groups.key
    }
  }
  /** Инициализирует и возвращает Ui
   * @returns {Ui} */
  static get ui() {
    //@ts-ignore
    return Registry._initOnce("ui", () => SpreadsheetApp.getUi());
  }

  /** Инициализирует и возвращает documentProperties
   * @returns {PropertiesService.Properties} */
  static get documentProperties() {
    return Registry._initOnce(
      "documentProperties",
      //@ts-ignore
      PropertiesService.getDocumentProperties())
  }

  //#region LivazeDb
  /** @returns {SpreadsheetApp.Spreadsheet} */
  static get dbSs() { return $._initOnce("dbSs", $.getSs(config.dbSsId)) }

  /** @returns { {  [sheetName: string]: { [key: string]: number} } } */
  static get dbKeys() {
    return $._initOnce("dbKeys", () => {
      if (!$.useSheetsConfig) return {}
      const sheet = $.livazeDbSs.getSheetByName("keys")
      if (!sheet) return {};
      return JSON.parse(sheet.getRange("A1").getValue())
    })
  }

  /**
   * @param {string } key 
   * @returns { DB }
   */
  static dbInitDb(key) {
    const dbConfig = config.db[key];
    if (!("ssId" in dbConfig)) dbConfig.ssId = config.dbSsId
    if (!("keys" in dbConfig)) dbConfig.keys = $.dbKeys[dbConfig.sheetName];
    return $._initOnce(key, new DB(dbConfig))
  }

  /**@returns {DB<msStockItem>} */
  static get msStockDB() {
    return $.dbInitDb("msStockDB");
  }

}
$.useSheetsConfig = true
/*export { $ }*/

