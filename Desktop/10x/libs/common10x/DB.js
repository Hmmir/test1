/*//@ts-check*/
/// <reference path="../../../index.d.ts" />
/** ORM для взаимодействия с листами Google Spreadsheets через объекты с данными
 * @template T
*/
class DB {
  /** 
    * @param { Object } parameters - параметры
    * @param { string } parameters.sheetName - название листа
    * @param { string } [parameters.ssId = "activeSpreadsheet"] - ssId таблицы
    * @param { number } [parameters.keysRow = 1] - номер строки с ключами
    * @param { number } [parameters.headersRow] - номер строки с заголовками
    * @param { number } [parameters.dataRowFirst = 2] - номер первой строки с данными
    * @param { number } [parameters.dataColumnFirst = 1] - номер первой колонки с данными
    * @param { number } [parameters.dataColumnsNum] - количество колонок с данными
    * @param { string[] | keys} [parameters.keys] - объект | массив с ключами
    * @param { { [format:string]: string} } [parameters.formats]
  */
  constructor({
    sheetName,
    ssId = "activeSpreadsheet",
    keysRow = 1,
    dataRowFirst = 2,
    dataColumnFirst = 1,
    headersRow = undefined,
    dataColumnsNum = undefined,
    keys = undefined,
    formats = undefined,
  }) {
    if (!sheetName) throw new Error("DB: sheetName is not defined")
    this.sheetName = sheetName;
    this.ssId = ssId;
    if (keys) this._validateAndSetKeys(keys);
    this.keysRow = keysRow;
    this.dataRowFirst = dataRowFirst;
    this.dataColumnFirst = dataColumnFirst;
    this.headersRow = headersRow;
    this.dataColumnsNum = dataColumnsNum;
    this._validateAndSetFormats(formats);
  }

  /** Вставляет данные на лист
   * @param { dataItems<T> } dataItems - массив объектов вида {"column_key": value}
   * @param { any } [defaultValue = undefined] - значение по умолчанию
   * @param { boolean } [growUp = false] - вставлять вверх
   */
  insertDataItems(dataItems, defaultValue = null, growUp = false) {
    this._checkDataItems(dataItems)
    const values = this._parseDataToArray({ dataItems, defaultValue });
    if (!values?.length) return
    if (growUp) {
      this.sheet.insertRowsBefore(this.dataRowFirst, values.length);
      this._setValues(this.dataRowFirst, this.dataColumnFirst, values);
    } else {
      const lastRow = this.sheet.getLastRow();
      this.sheet.insertRowsAfter(lastRow < this.dataRowFirst ? this.dataRowFirst : lastRow, values.length);
      this._setValues(lastRow < this.dataRowFirst ? this.dataRowFirst : lastRow + 1, this.dataColumnFirst, values);
    }
    this.setFormats()
    this.resetCache()
  }

  /** Вставляет данные на лист
   * @param { dataItem<T> } dataItem - объект вида {"column_key": value}
   * @param { any } [defaultValue = undefined] - значение по умолчанию
   * @param { boolean } [growUp = false] - вставлять вверх
   */
  insertDataItem(dataItem, defaultValue = null, growUp = false) {
    this.insertDataItems([dataItem], defaultValue, growUp)
  }

  /** Вставляет данные на лист
   * @param { dataItemsArray<T> } dataItems - массив объектов вида {"column_key": value}
   * @param { any } [defaultValue = undefined] - значение по умолчанию
   * @param { boolean } [growUp = false] - вставлять вверх
   */
  insert(dataItems, defaultValue = "", growUp = false) {
    this.insertDataItems(dataItems, defaultValue, growUp)
  }

  /** Вставляет чекбоксы в колонки по ключу | ключам
   * @param { string | string[] } keys - ключ | ключи колонок для вставки чекбоксов
   */
  insertCheckboxes(keys) {
    keys = [...[keys].flat()]
    this._checkKeys(keys);
    for (let key of keys) {
      this.sheet.getRange(this.dataRowFirst, this.keysColumns[key], this.sheet.getMaxRows() - this.dataRowFirst + 1 - 1).insertCheckboxes();
    }
    this.resetCache()
  }

  /** Возвращает массив содержащий объекты с данными
    * @param { Object } [options]
    *   @param { (item: dbItem<T> ) => boolean } [options.filterCB] - функция фильтрации, применяемая на этапе наполнения объектами
    *   @param { (item: dbItem<T> ) => void } [options.itemCB] - функция вызываемая для каждого элемента перед добавлением в объект
    *   @param { string | string[] } [options.ignoreKeys] - ключ / список ключей для исключения
    * @returns { dbItemsArray<T> }
    */
  getDataItems({ filterCB = undefined, itemCB = undefined, ignoreKeys = undefined } = {}) {

    return this.getDataItemsArray({ filterCB, itemCB, ignoreKeys });
  }

  /** Возвращает массив содержащий объекты с данными
    * @param { Object } [options]
    *   @param { (item: dbItem<T> ) => boolean } [options.filterCB] - функция фильтрации, применяемая на этапе наполнения объектами
    *   @param { (item: dbItem<T> ) => void } [options.itemCB] - функция вызываемая для каждого элемента перед добавлением в объект
    *   @param { string | string[] } [options.ignoreKeys] - ключ / список ключей для исключения
    * @returns { dbItemsArray<T> }
    */
  getDataItemsArray({ filterCB = undefined, itemCB = undefined, ignoreKeys = undefined } = {}) {
    const keys = ignoreKeys === undefined ? this.keys : this._filterKeysFrom([...[ignoreKeys ?? []].flat()]);
    const values = this.values;
    const dbItems = this.dbItems;
    const vl = values.length;
    const items = [];
    for (let i = 0, key, item, dbItem; i < vl; i++) {

      if (this._rowsEmptyStatuses && this._rowsEmptyStatuses[i]) continue
      dbItem = dbItems[i]
      if (filterCB !== undefined && !filterCB(dbItem)) continue;

      if (ignoreKeys === undefined) {

        item = Object.assign(new DBItem(), dbItem)
      } else {
        item = new DBItem();
        for (key in keys) { item[key] = dbItem[key]; }

        item.row = dbItem.row;
      }

      //@ts-ignore
      if (itemCB !== undefined) itemCB(item);
      items.push(item)
    }
    //@ts-ignore
    return items;
  }

  /**
   * Возвращает объект содержащий объекты с данными
   *
   * @param { string | string[] } useKeys - Задает ключ | ключи, для генерации hashKey
   * @param { Object } [options]
   *   @param { (item: dbItem<T>) => boolean } [options.filterCB] - функция фильтрации, применяемая на этапе наполнения объектами
   *   @param { (item: dbItem<T>, hashKey: string ) => void } [options.itemCB] - функция вызываемая для каждого элемента перед добавлением в объект
   *   @param { (hashKey: string) => string } [options.keyCB] - функция преобразования ключа
   *   @param { string | string[] } [options.ignoreKeys] - список ключей для исключения
   * @return { dbItemsObject<T> } A DbItemsObject containing the generated data items.
   */
  getDataItemsObject(useKeys, { filterCB = undefined, itemCB = undefined, keyCB = undefined, ignoreKeys = undefined } = {}) {
    const keys = ignoreKeys === undefined ? this.keys : this._filterKeysFrom([...[ignoreKeys ?? []].flat()]);
    const values = this.values;
    const dbItems = this.dbItems;
    const items = new DBItemsObject();
    const vl = values.length
    const hashFunction = this._getKeysHashFunction(useKeys);

    for (let i = 0, key, item, dbItem, hashKey; i < vl; i++) {
      if (this._rowsEmptyStatuses && this._rowsEmptyStatuses[i]) continue
      dbItem = dbItems[i]
      if (filterCB !== undefined && !filterCB(this.dbItems[i])) continue
      hashKey = keyCB === undefined ? hashFunction(dbItem) : keyCB(hashFunction(dbItem));

      if (ignoreKeys === undefined) {
        item = Object.assign(new DBItem(), dbItem)
      } else {
        item = new DBItem();
        for (key in keys) { item[key] = dbItem[key]; }

        item.row = dbItem.row;
      }

      //@ts-ignore
      if (itemCB !== undefined) itemCB(item, hashKey);
      items[hashKey] = item;
    }
    //@ts-ignore
    return items;
  }

  /** Обновляет переданные dataItems на листе, непрерывными чанками.
   * Подразумевается получение объектов из таблицы при помощи get методов, их последующая модификация,
   * и возвращение в таблицу
   * @param { dbItems<T> } dbItems - объекты полученные из таблицы
   * @throws { TypeError }
   */
  updateDataItems(dbItems) {
    const keys = this.keys
    const currentItems = this.dbItems
    const dataRowFirst = this.dataRowFirst
    /** @type { dbItemsArray<T> } *///@ts-ignore
    const itemsArray = this._checkDBItems(dbItems) === "object" ? Object.values(dbItems) : dbItems
    itemsArray.sort((a, b) => a.row - b.row)
    if (!itemsArray.length) return;
    for (let i = 0, il = itemsArray.length, dbItem, key; i < il; i++) {
      dbItem = currentItems[itemsArray[i].row - dataRowFirst]
      for (key in itemsArray[i]) {
        //@ts-ignore
        if (key in keys) dbItem[key] = itemsArray[i][key]
      }
      itemsArray[i] = dbItem
    }
    this._updateDataItemsByChunks(itemsArray)
    this.resetCache()
    this.setFormats()
  }

  /**Обновляет чанками подготовленный массив dataItems
   * @private
   * @param { dbItem<T>[] } itemsArray - массив c dataItems
   */
  _updateDataItemsByChunks(itemsArray) {
    const chunks = [{ firstRow: itemsArray[0].row, lastRow: itemsArray[0].row, indexes: [0] }]
    if (itemsArray.length > 1) {
      let c = 0
      for (let i = 1, il = itemsArray.length; i < il; i++) {
        if (itemsArray[i].row - chunks[c].lastRow !== 1) {
          chunks.push({ firstRow: itemsArray[i].row, lastRow: itemsArray[i].row, indexes: [i] })
          c++;
        } else {
          chunks[c].lastRow = itemsArray[i].row;
          chunks[c].indexes.push(i)
        }
      }
    }
    let chunk, chunkItems, chunkItemsArray;
    for (chunk of chunks) {
      chunkItems = chunk.indexes.map(i => itemsArray[i])
      chunkItemsArray = this._parseDataToArray({ dataItems: chunkItems })
      this._setValues(chunk.firstRow, this.dataColumnFirst, chunkItemsArray);
    }
  }

  /** Возвращает один объект. 
   * В отличие от других функций обращается к диаппазону конкретной строки напрямую.
   * @param { number } row - номер строки
   * @param { object } [options]
   * @param { (item: dbItem<T>) => void } [options.itemCB] - функция для изменения объекта
   * @param { string | string[] } [options.ignoreKeys]  - массив ключей не включаемых в возвращаемый объект
   * @returns { dbItem<T> }
   */
  getDataItemByRow(row, { ignoreKeys = undefined, itemCB = undefined } = {}) {
    const keys = ignoreKeys === undefined ? this.keys : this._filterKeysFrom([...[ignoreKeys ?? []].flat()]);
    let item;
    item = this._parseRowValuesToItem({
      values: this.sheet.getSheetValues(row, this.dataColumnFirst, 1, this.dataColumnsNum ?? this.lastDataColumn - this.dataColumnFirst + 1)[0],
      row,
      keys
    });
    if (itemCB !== undefined) itemCB(item);
    return item;
  }

  /** Обновляет dataItem в указанной строке. 
   * Значения указанные в ignoreKeys останутся прежними.
   * Значения отсутствующие в dataItem останутся прежними.
   * Если переданы controlKeys и было обнаружено несовпадение - вернет false.
   * @param { number } row - номер строки
   * @param { dataItem<T> } dataItem - объект с данными
   * @param { object } [options] - настройки
   * @param { string | string[] } [options.controlKeys] - массив ключей для проверки подлинности объекта
   * @param { string | string[] } [options.ignoreKeys]  - массив ключей значения которых останутся прежними
   * @returns { boolean } - успешность выполнения обновления
   */
  updateDataItemByRow(row, dataItem, { controlKeys = undefined, ignoreKeys = undefined } = {}) {
    const currentDataItem = this.getDataItemByRow(row);
    if (controlKeys !== undefined && controlKeys?.length > 0) {
      for (let key of controlKeys) {
        if (currentDataItem[key] !== dataItem[key]) {
          return false
        }
      }
    }

    const keys = this._filterKeysFrom([...[controlKeys ?? []].flat(), ...[ignoreKeys ?? []].flat()]);
    for (let key in keys) {
      if (key in dataItem) {
        //@ts-ignore
        currentDataItem[key] = dataItem[key];
      }
    }

    const values = this._parseDataToArray({ dataItems: [currentDataItem] });
    this._setValues(currentDataItem.row, this.dataColumnFirst, values);
    this.resetCache()
    this.setFormats()
    return true;
  }

  /** Проверяет является ли dataItems экземпляром "DBItemsObject" или массивом 
   * @private
   * @param {  dbItemsArray<T> | dbItemsObject<T> } dataItems - объекты для слияния с текущими объектами таблицы
   * @throws { TypeError }
   * @return { "array" | "object" }
  */
  _checkDBItems(dataItems) {
    if (dataItems instanceof DBItemsObject) {
      return 'object'
    } else if (Array.isArray(dataItems)) {
      if (dataItems.some(i => !(i instanceof DBItem))) throw TypeError(`DB: Invalid dataItems type in dataItems array`);
      return 'array'
    }
    throw new TypeError(`DB: updatedItems has invalid type (${typeof dataItems})`)

  }

  /** Проверяет является ли dataItems "array" или экземпляром "DBItemsObject"
   * @private
   * @param { dataItemsArray<T> | dataItemsObject<T> } dataItems - объекты для слияния с текущими объектами таблицы
   * @throws { TypeError }
   * @return { "array" | "object" }
  */
  _checkDataItems(dataItems) {
    if (Array.isArray(dataItems)) {
      if (dataItems.every(i => typeof i == "object")) return 'array'
    } else if (typeof dataItems == "object") {
      if (Object.values(dataItems).every(i => !Array.isArray(i) && typeof i == "object")) return "object"
    }
    throw new TypeError(`DB: updatedItems has invalid type (${typeof dataItems})`)
  }

  /** Производит слияние переданных объектов с текущими объектами таблицы. 
   * Обновляет, вставляет и удаляет данные в соостветствии с переданными callback функциями.
   * Без передачи дополнительных опций будет искать обновленные данные по всем ключам кроме переданных в useKeys.
   * @param { dataItems<T>  } dataItems - объекты для слияния с текущими объектами таблицы
   * @param { string | string[] } useKeys - ключ | комбинация ключей определяющих уникальность объекта, на их основе будет создан комбинированый ключ
   * @param { Object } [options] - объект позволяющий настроить процесс слияния.
   *   @param { string[] } [options.ignoreKeys] - список ключей игнорируемых при сравнении и слиянии объектов
   *   @param { (currentItem: dbItem<T> ) => boolean } [options.filterCB] - функция фильтрации, применяется для выборки currentItem объектов для сравнения с новыми объектами;
   *   @param { (hashKey: string) => string } [options.keyCB] - функция вызываемая для каждого hashKey после его создания
   *   @param { (currentItem: dbItem<T> , newItem: dataItem<T> ) => boolean } [options.isUpdateCB] - функция для проверки необходимости обновления currentItem на основе newItem
   *   @param { (currentItem:  dbItem<T> , newItem: dataItem<T> ) => void } [options.updateCB]  - функция для модификации currentItem на основе newItem перед вставкой на лист
   *   @param { (currentItem: dbItem<T> ) => boolean } [options.isDeleteCB] - функция для проверки необходимости удаления dataItem
   * @return { { updated: number, inserted: number, deleted: number } }
   */
  mergeDataItems(dataItems, useKeys, { isUpdateCB = undefined, updateCB = undefined, isDeleteCB = undefined, filterCB = undefined, keyCB = undefined, ignoreKeys = undefined } = {}) {

    const currentItemsObj = this.getDataItemsObject(useKeys, { filterCB, keyCB });
    const itemsToUpdate = new DBItemsObject();
    const itemsToInsert = new DBItemsObject();
    const itemsToDelete = new DBItemsObject();

    let key;
    const comparsionKeys = this._filterKeysFrom([...[useKeys].flat(), ...[ignoreKeys ?? []].flat()]);
    if (isUpdateCB === undefined) isUpdateCB = (c, n) => {
      for (key in comparsionKeys) { if (key in n && c[key] != n[key]) return true }; return false;
    }
    if (updateCB === undefined) updateCB = (c, n) => {
      //@ts-ignore
      for (key in comparsionKeys) { if (key in n) c[key] = n[key]; }
    }

    //@ts-ignore
    const dataItemsObj = this._checkDataItems(dataItems) === "object" ? dataItems : this._parseDataItemsArrayToObject(dataItems, useKeys, keyCB);

    let hash, currentItem, item, updated = 0, inserted = 0, deleted = 0;
    for (hash in dataItemsObj) {
      item = dataItemsObj[hash]
      if (hash in currentItemsObj) {
        currentItem = currentItemsObj[hash];
        if (isUpdateCB(currentItem, item)) {
          updateCB(currentItem, item);
          itemsToUpdate[hash] = currentItem
          updated++
        }
      } else {
        itemsToInsert[hash] = item;
        inserted++
      }
    }
    if (isDeleteCB !== undefined) {
      for (hash in currentItemsObj) {
        if (!(hash in itemsToUpdate) && !(hash in dataItemsObj)) {
          currentItem = currentItemsObj[hash]
          if (isDeleteCB(currentItem)) {
            itemsToDelete[hash] = currentItem
            deleted++
          }
        }
      }
    }
    if (updated) this._updateDataItemsByChunks(Object.values(itemsToUpdate).sort((a, b) => a.row - b.row));
    //@ts-ignore
    if (inserted) this.insertDataItems(itemsToInsert);
    //@ts-ignore
    if (deleted) this.deleteDataItems(itemsToDelete);
    this.resetCache()
    this.setFormats()
    return { updated, inserted, deleted }
  }

  /** Заменяет все данные на листе
   * @param { dataItems<T> } dataItems - массив c dataItems | объект с dataItems
   * @param { any } [defaultValue = undefined] - значение по умолчанию
   */
  replaceData(dataItems, defaultValue = null) {
    this._checkDataItems(dataItems)
    //@ts-ignore
    this._fit(this._isObject(dataItems) ? Object.keys(dataItems).length : dataItems.length);
    this._setValues(this.dataRowFirst, this.dataColumnFirst, this._parseDataToArray({ dataItems, defaultValue }));
    this.resetCache()
    this.setFormats()
  }

  /** Удаляет непрерывными чанками, переданные dbItems с листа.
   * @param { dbItems<T> } dbItems - объекты полученные из таблицы
   */
  deleteDataItems(dbItems) {
    /** @type { dbItemsArray<T> } *///@ts-ignore
    const itemsArray = (this._checkDBItems(dbItems) === "object" ? Object.values(dbItems) : dbItems)
    itemsArray.sort((a, b) => a.row > b.row ? 1 : -1)
    if (!itemsArray.length) return;
    this._deleteDataItemsByChunks(itemsArray)
    this.resetCache()
    this.setFormats()
  }

  /** Удаляет чанками подготовленный массив dataItems
   * @private
   * @param { dbItem<T>[] } itemsArray - объекты полученные из таблицы и модифицированные
   */
  _deleteDataItemsByChunks(itemsArray) {
    const chunks = [{ firstRow: itemsArray[itemsArray.length - 1].row, lastRow: itemsArray[itemsArray.length - 1].row }]
    if (itemsArray.length > 1) {
      let c = 0
      for (let i = itemsArray.length - 2; i >= 0; i--) {
        if (chunks[c].firstRow - itemsArray[i].row !== 1) {
          chunks.push({ firstRow: itemsArray[i].row, lastRow: itemsArray[i].row })
          c++;
        } else {
          chunks[c].firstRow = itemsArray[i].row;
        }
      }
    }
    let chunk;
    for (chunk of chunks) {
      this.sheet.deleteRows(chunk.firstRow, chunk.lastRow - chunk.firstRow + 1)
    }
  }

  /** Возвращает объект содержащий ключи и индексы расположения в массиве this.values
   * @returns { T & { [key:string] : number } }
  */
  get keys() {
    return this._isInit("keys", () => this._getKeysFromSheet());
  }
  /** Возвращает объект содержащий ключи и номера колонок на листе
   * @returns { keys }
   */
  get keysColumns() {
    return this._isInit("keysColumns", () => {
      const keysColumns = {}
      for (let key in this.keys) {
        keysColumns[key] = (this.keys[key] + 1);
      }
      return keysColumns;
    });
  }

  /** Возвращает объект содержащий ключи и буквенные обозначения колонок на листе
   * @returns { T & { [key:string] : string } }
   */
  get keysLetters() {
    return this._isInit("keysLetters", () => {
      const keysLetters = {}
      for (let key in this.keys) {
        keysLetters[key] = this.letter(this.keys[key] + 1);
      }
      return keysLetters;
    });
  }

  /** Возвращает массив с ключами
   * @returns { string [] }
   */
  get keysArray() {
    return this._isInit("keysArray", Object.keys(this.keys))
  }

  /** Возвращает индекс последней колонки с данными на основе максимального значения из this.keysColumns
   * @returns { number } */
  get lastDataColumn() {
    return this._isInit("lastDataColumn", () => Math.max(...Object.values(this.keysColumns)));
  }

  /** сбрасывает сохраненные значаения keys, keysArray, keysColumns, keysLetters, lastDataColumn 
   * @private
  */
  resetKeys() {
    this._reset("keys")
    this._reset("keysArray")
    this._reset("keysColumns")
    this._reset("keysLetters")
    this._reset("lastDataColumn")
  };

  /** вызывает clearContent() для диаппазона с данными */
  clearData() {
    this.sheet.getRange(`${this.letter(this.dataColumnFirst)}${this.dataRowFirst}`
      + `:${this.letter(this.dataColumnsNum ? (this.dataColumnFirst + this.dataColumnsNum - 1) : this.lastDataColumn)}`
    ).clearContent();
    this.resetCache();
  }

  /** Выполняет сортировку данных в диаппазоне с данными при помощи range.sort(sortSpecObj)
   * @param { { [key: string] : "asc" | "desc" } } sortOptions - объект вида { [key: string] : "asc" | "desc" }
   */
  sort(sortOptions) {
    const sortSpecObj = []
    for (let key in sortOptions) {
      this._checkKeys(key);
      if (sortOptions[key] != "asc" && sortOptions[key] != "desc") throw new Error(`DB: Invalid sort direction "${sortOptions[key]}"`)
      sortSpecObj.push({
        column: this.keysColumns[key] - this.dataColumnFirst + 1,
        ascending: sortOptions[key] == "asc"
      })
    }
    this.sheet.getRange(
      this.dataRowFirst,
      this.dataColumnFirst,
      this.sheet.getMaxRows() - this.dataRowFirst + 1 - 1,
      this.dataColumnsNum ?? this.lastDataColumn - this.dataColumnFirst + 1).sort(sortSpecObj)
    this.resetCache();
  }

  /** Изменяет размер листа в зависимости от количества данных
   * @private
   * @param { number } dataRowsNum
   * @param { boolean } [growUp = false] - вставлять вверх
   * */
  _fit(dataRowsNum, growUp = false) {
    const maxRowIndex = this.sheet.getMaxRows() - 1;
    // const funcTimeMessage = `fit(dataRowsNum = ${dataRowsNum}) maxRowIndex = ${maxRowIndex}`;
    //prettier-ignore
    const difference = (maxRowIndex - this.dataRowFirst + 1) - dataRowsNum;
    if (difference != 0) {
      if (difference > 0) {
        this.sheet.deleteRows(maxRowIndex - difference + 1, difference);
      } else {
        if (growUp) { this.sheet.insertRowsBefore(this.dataRowFirst, -difference); }
        else { this.sheet.insertRowsAfter(maxRowIndex, -difference); }
      }
      this.resetCache();
    }
  }

  /** Проверяет есть ли данные в объекте
   * @private
   * @param { dataItem<T> | dbItem<T>[] } dataItem 
   * @returns { boolean }
   */
  _isItemHasData(dataItem) {
    for (let key in dataItem) {
      if (key == "row") continue;
      if (dataItem[key] != null) return true;
    }
    return false
  }

  /** Проверяет является ли item объектом
   * @private
   * @param { dataItem<T>[] | dataItemsObject<T> | dbItem<T>[] | dbItemsObject < dbItem<T> > } dataItems 
   * @returns { boolean }
   */
  _isObject(dataItems) {
    return typeof dataItems == "object" && !Array.isArray(dataItems)
  }

  /** Конвертирует строку в объект
   * @private
   * @param { Object } params - объект с параметрами
   * @param { any[] } params.values - массив строки
   * @param { number } params.row - номер строки
   * @param { keys } [params.keys] - объект с ключами, по умолчанию this.keys
   * @returns { dbItem<T> }
   */
  _parseRowValuesToItem({ values, row, keys = this.keys }) {
    const item = new DBItem;
    let key
    for (key in keys) {
      if (values[keys[key]] === "") {
        item[key] = null
      } else {
        item[key] = values[keys[key]];
      }
    }
    item.row = row;
    //@ts-ignore
    return item;
  }

  /** Конвертирует массив объектов в массив для вставки
   * @private
   * @param { Object } options - массив объектов/объект с объектами
   * @param { dataItemsArray<T> | dataItemsObject<T> } options.dataItems - массив объектов/объект с объектами
   * @param { any } [options.defaultValue=null] - значение по умолчанию (null)
   * @returns { any[][] }
   */
  _parseDataToArray({ dataItems, defaultValue = null }) {
    const keys = this.keys;
    let i;
    let key;
    if (Array.isArray(dataItems)) {
      const dataArray = this._getEmptyArray(dataItems.length, this.lastDataColumn - this.dataColumnFirst + 1, defaultValue);
      const iLength = dataItems.length;
      for (i = 0; i < iLength; i++) {
        for (key in keys) {
          if (key in dataItems[i]) dataArray[i][keys[key]] = dataItems[i][key];
        }
      }
      return dataArray;
    } else {
      const dataArray = this._getEmptyArray(Object.keys(dataItems).length, this.lastDataColumn - this.dataColumnFirst + 1, defaultValue);
      let n = 0;
      for (i in dataItems) {
        for (key in keys) {
          if (key in dataItems[i]) dataArray[n][keys[key]] = dataItems[i][key];
        }
        n++;
      }
      return dataArray;
    }
  }

  /** Создает на основе массива с объектами, объект содержащий объекты с данными в виде ключ-значение
   * @private
   * @param { dataItemsArray<T> | dbItemsArray<T> } dataItems - массив объектов вида {"column_key": value}
   * @param { string | string[] } useKeys - ключ | ключи, по которым будет собран объект
   * @param { (hashKey: string) => string } [keyCB] - функция преобразования ключа
   * @returns { dbItemsObject < dbItem<T> > }
   */
  _parseDataItemsArrayToObject(dataItems, useKeys, keyCB = undefined) {
    const hashFunction = this._getKeysHashFunction(useKeys);
    const dataItemsObject = new DBItemsObject();
    for (let i = 0, il = dataItems.length, hashKey; i < il; i++) {
      if (keyCB !== undefined) { hashKey = keyCB(hashFunction(dataItems[i])); }
      else { hashKey = hashFunction(dataItems[i]); }
      if (hashKey in dataItemsObject) throw new Error(`Parse DataItemsArray to DataItemsObject error\nHash "${hashKey}" already exists in dataItemsObject`);
      dataItemsObject[hashKey] = dataItems[i];
    }
    //@ts-ignore
    return dataItemsObject;
  }

  /** Получает ключи из строки this.dataRowFirst  
   * @private
   * @returns { keys }
   */
  _getKeysFromSheet() {
    const range = this.sheet
      .getRange(
        this.letter(this.dataColumnFirst) + this.keysRow + ":"
        + (this.dataColumnsNum ? this.letter(this.dataColumnFirst + this.dataColumnsNum - 1) : "")
        + this.keysRow
      );

    const keys = range.getValues()[0].reduce((acc, value, index) => {
      if (value) {
        acc[value] = index;
      }
      return acc;
    }, {})
    if (!this._isItemHasData(keys)) throw new Error(`DB: No keys found on sheet: "${this.sheetName}" row: ${this.dataRowFirst} `)
    return keys;
  }

  /** Проверяет ключи и устанавливает в this.keys без возможности для изменения 
   * @private
   * @param { { [key:string] : number } | string[] } keys - ключи
   * 
  */
  _validateAndSetKeys(keys) {
    if (typeof keys != 'object' && !Array.isArray(keys)) throw new TypeError("keys must be an object or array");
    if (Array.isArray(keys)) {
      if (!keys.length) throw new Error(`keys: ${JSON.stringify(keys, null, 2)}\nHave no keys`)
      const keysObject = {};
      for (let i = 0; i < keys.length; i++) {
        if (keys[i] !== undefined || keys[i] !== null) {
          if (keys[i] in keysObject) throw new Error(`keys array:[${keys.map(i => `"${i}"`).join(", ")}]\nmust contain only unique values`)
          keysObject[keys[i]] = i;
        }
      }
      this._initOnce("keys", keysObject);
    } else {
      if (!Object.keys(keys).length) throw new Error(`keys: ${JSON.stringify(keys, null, 2)}\nHave no keys`)
      for (let key in keys) {
        if (typeof keys[key] != 'number') throw new TypeError(`key: "${key}" value (${keys[key]}) is not a number`);
        if (keys[key] < 0) throw new Error(`key: "${key}" value (${keys[key]}) can not be less then 0`);
      }
      this._initOnce("keys", keys);
    }
    this._initOnce("resetKeys", () => () => { throw new Error("keys are imutable") });
  }

  /**
   * @param { { [format:string]:string } } [formats]
   * @returns 
   */
  _validateAndSetFormats(formats) {
    if (this.formats === undefined) return this.formats = formats;
    if (typeof formats != 'object') throw new TypeError("keys must be an object");
    for (let key in formats) {
      if (typeof formats[key] != 'string') throw new TypeError(`Formats["${key}"]:"${formats[key]}" is not a string`);
    }

    this.formats = formats;
  }
  setFormats() {
    if (this.formats === undefined) return;
    const letters = this.keysLetters;
    let key;
    for (key in this.formats) {
      if (!(key in this.keys)) throw new Error(`No key ${key} in keys` + `\nkeys: ${JSON.stringify(this.keys)}` + `\nformats: ${JSON.stringify(this.formats)}`);
      this.sheet.getRange(`${letters[key]}${this.dataRowFirst}:${letters[key]}`).setNumberFormat(this.formats[key])
    }
  }

  /** Возвращает функцию для объединения ключей
   * @param { string | string[] } useKeys - ключ | ключи, для создания хэш-функци 
   * @returns { (item: dataItem<T>) => string } - функция преобразования ключа { (item: dbItem<T>) => string }
   */
  _getKeysHashFunction(useKeys) {
    this._checkKeys(useKeys);
    if (typeof useKeys == "string") {
      return (item) => item[useKeys];
    } else if (Array.isArray(useKeys) && useKeys.length == 1) {
      return (item) => item[useKeys[0]];
    } else if (Array.isArray(useKeys) && useKeys.length > 1) {
      const il = useKeys.length
      let i;
      /**@type { string } */
      let hash
      return (item) => {
        hash = item[useKeys[0]];
        for (i = 1; i < il; i++) hash += `|${item[useKeys[i]]}`;
        return hash;
      }
    }
    throw new Error("Invalid useKeys")
  }

  /** Проверяет наличие ключа | ключей в таблице
   * @private
   * @param { string | string[] } keys - ключ | массив ключей
   * @throws { Error }
   * @returns { void }
   */
  _checkKeys(keys) {
    if (typeof keys == "string") {
      if (!(keys in this.keys)) throw new Error(`DB: No '${keys}' key in '${this.sheetName}'`);
    } else if (Array.isArray(keys) && keys.length > 0) {
      for (let key of keys) {
        if (!(key in this.keys)) throw new Error(`DB: No '${key}' key in '${this.sheetName}'`);
      }
    } else {
      throw new Error(`DB: "Invalid keys"`);
    }
  }

  /** Собирает объект с ключами, исключая ignoreKeys
   * @private
   * @param { string[] } [ ignoreKeys ] - массив строки
   * @returns { keys }
   * */
  _filterKeysFrom(ignoreKeys = undefined) {
    if (ignoreKeys === undefined) return this.keys;
    /**@type {keys} */
    const keys = {};
    for (let key in this.keys) {
      if (!ignoreKeys.includes(key)) {
        keys[key] = this.keys[key];
      }
    }

    return keys
  }

  /**Возвращает массив заполненный filler
   * @private
   * @param { number } rows - количество строк
   * @param { number } cols - количество колонок
   * @param { any } [filler] - заполнитель 
   * @returns { any[][] } */
  _getEmptyArray(rows, cols, filler = null) {
    const array = Array(rows);
    const row = Array(cols);
    row.fill(filler);
    for (let r = 0; r < rows; r++) {
      array[r] = row.slice(0);
    }
    return array;
  }

  /** Возвращает букву колонки на основе номера 
   * @private
   * @param { number } columnNumber - номеро колонки 
   * @returns { string } */
  letter(columnNumber) {
    if (columnNumber < 1) throw new Error(`Wrong column number (${columnNumber})`)
    let remain,
      string = "";
    if (columnNumber < 27) return String.fromCharCode(64 + columnNumber);
    do {
      remain = columnNumber % 26;
      columnNumber = Math.floor(columnNumber / 26);
      string = String.fromCharCode(64 + remain) + string;
    } while (columnNumber > 0);
    return string;
  }

  /** Генерирует заготовку описания типа "@typedef" на основе ключей и заголовков таблицы 
   * @param { string } [itemName] - имя типа
   * @returns { string }
  */
  generateTypeDef(itemName = undefined) {
    const types = []
    for (const key in this.keys) {
      if (key) {
        types.push(` * @property { any } ${key} - ${this.headersRow ? this.sheet.getRange(this.headersRow, this.keys[key] + 1).getValue() : ""}`)
      }
    }
    return `/** @typedef {Object} ${itemName ?? `${this.sheetName}Item`}\n` + types.join('\n') + "\n*/"
  }

  /** Устанавливает значения в ячейки начиная с row, col согласно размера массива values
   * @private
   * @param { number } row
   * @param { number } col
   * @param { any[][] } values
   */
  _setValues(row, col, values) {
    if (!values?.length) return false;
    if (!values[0]?.length) return false;
    this.sheet.getRange(row, col, values.length, values[0].length).setValues(values);
    return true;
  }

  /** @returns { any[][] } - массив значений dataRange с листа files_queque */
  get values() {
    return this._isInit("values", () => {
      return this.sheet.getRange(`${this.letter(this.dataColumnFirst)}${this.dataRowFirst}:${this.letter(this.dataColumnsNum ? (this.dataColumnFirst + this.dataColumnsNum - 1) : this.lastDataColumn)}`).getValues()
    });
  }

  /** 
   * @returns { dbItemsArray<T> } - массив объектов с данными 
  */
  get dbItems() {
    return this._isInit("dbItems", () => {
      const values = this.values;
      this._rowsEmptyStatuses = values.map(row => row.every(v => v === ""))
      const objects = []
      const keys = this.keys
      const firstRow = this.dataRowFirst
      if (!values?.length) return [];
      let i, il = values.length, key
      for (i = 0; i < il; i++) {
        objects[i] = new DBItem()
        for (key in keys) {
          objects[i][key] = values[i][keys[key]]
        }
        objects[i].row = i + firstRow
      }
      return objects
    });
  }

  /**Сбрасывает значения values и dbItems 
   * @private
  */
  resetCache() {
    this._reset("values")
    this._reset("dbItems")
    this._rowsEmptyStatuses = undefined
  }

  /** @returns { SpreadsheetApp.Sheet} */
  get sheet() {
    return this._initOnce("sheet", DB._getSheet(this.sheetName, this.ssId));
  }

  /** @returns { SpreadsheetApp.Spreadsheet } */
  get ss() {
    return this._initOnce("ss", DB._getSs(this.ssId))
  }

  /** Получает и сохраняет ss в sheetsMap по ssId и sheetName
   * @private
   * @param { string } sheetName
   * @param { string } ssId
   * @returns { SpreadsheetApp.Sheet } */
  static _getSheet(sheetName, ssId) {
    const hash = ssId + "." + sheetName;

    if (!DB._sheetsMap.has(hash) || DB._sheetsMap.get(hash) === null) {
      DB._sheetsMap.set(hash, DB._getSs(ssId).getSheetByName(sheetName)
      );
    }
    return DB._sheetsMap.get(hash);
  }

  /** Map для хранения ссылок на листы
   * @private
   * @returns { Map } */
  static get _sheetsMap() {
    return DB._initOnce("_sheetsMap", () => new Map())
  }

  /** Получает и сохраняет ss в ssMap по ssId
   * @private
   * @param { string } ssId
   * @returns { SpreadsheetApp.Spreadsheet } */
  static _getSs(ssId) {
    if (!DB._ssMap.has(ssId)) {
      if (ssId === "activeSpreadsheet") {
        DB._ssMap.set(ssId, SpreadsheetApp.getActiveSpreadsheet());
      } else {
        DB._ssMap.set(ssId, SpreadsheetApp.openById(ssId));
      }
    }
    return DB._ssMap.get(ssId);
  }

  /**  Map для хранения ссылок на таблицы
   * @private
   * @returns { Map } */
  static get _ssMap() {
    return DB._initOnce("_ssMap", () => new Map())
  }

  /** Инициализирует свойства класса или возвращает инициализированное значение
   * @private
   * @param { string } key - ключ по которому происходит обращение, property_name.
   * @param { function | any } valueOrCallback - значение или callback возвращающий значение для инициализации;
   * @returns { any } - значение valueOrCallback;
   */
  static _isInit(key, valueOrCallback) {
    if (this["_" + key] === undefined) {
      this["_" + key] = typeof valueOrCallback == "function" ? valueOrCallback() : valueOrCallback
    }
    return this["_" + key];
  }

  /** Инициализирует свойства класса или возвращает инициализированное значение
   * @private
   * @param { string } key - ключ по которому происходит обращение, property_name.
   * @param { function | any } valueOrCallback - значение или callback возвращающий значение для инициализации;
   * @returns { any } - значение valueOrCallback;
   */
  _isInit(key, valueOrCallback) {
    if (this["_" + key] === undefined) {
      this["_" + key] = typeof valueOrCallback == "function" ? valueOrCallback() : valueOrCallback
    }
    return this["_" + key];
  }

  /** Инициализирует свойства класса и заменяет геттер свойством, блокируя перезапись.
   * @private
   * @param { string } key - ключ по которому происходит обращение, property_name.
   * @param { function | any } valueOrCallback - значение или callback возвращающий значение для инициализации;
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

  /** Инициализирует свойства класса и заменяет геттер свойством, блокируя перезапись.
   * @private
   * @param { string } key - ключ по которому происходит обращение, property_name.
   * @param { function | any } valueOrCallback - значение или callback возвращающий значение для инициализации;
   * @returns { any } - значение valueOrCallback;
   */
  _initOnce(key, valueOrCallback) {
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
   * @private
   * @param { string } key - ключ по которому происходит обращение, property_name.
   */
  static _reset(key) {
    this["_" + key] = undefined;
  }

  /** Сбрасывает свойство класса.
   * Используется например для сброса range после операций вставки.
   * @private
   * @param { string } key - ключ по которому происходит обращение, property_name.
   */
  _reset(key) {
    this["_" + key] = undefined;
  }
}

/** Содержит данные с листа в виде ключ-значение
 * @typedef { { [key: string]: number} } keys
*/
/** Содержит данные с листа в виде ключ-значение
 * @template T
 * @typedef { T & { [key: string]: any} } dataItem
*/
/** Содержит данные с листа в виде ключ-значение
 * @template T
 * @typedef { dataItem<T>[] } dataItemsArray
*/
/** Содержит данные с листа в виде ключ-значение
 * @template T
 * @typedef { { [hash: string]: dataItem<T> } } dataItemsObject
*/

/** Содержит данные с листа в виде ключ-значение
 * @template T
 * @typedef { dataItem<T> & DBItem<T> & { row: number }} dbItem
*/

/** Массив с объектами DBItem
 * @template T
 * @typedef { dbItem<T>[] } dbItemsArray
*/

/** Объект с объектами DBItem
 * @template T
 * @typedef { { [hash: string]: dbItem<T> } & DBItemsObject<T> } dbItemsObject
*/

/** Любой из доступных типов, содержащий объекты с данными
 * @template T
 * @typedef { dataItemsArray<T> | dataItemsObject<T> | dbItemsArray<T> | dbItemsObject<T> } dataItems
*/

/** Типы возвращаемые функциями класса DB
 * @template T
 * @typedef { dbItemsArray<T> | dbItemsObject<T> } dbItems
*/
/** Класс для создания объектов, содержащих объекты с данными 
 * @template T
 * @property {number} row
*/
class DBItem {
  constructor() {
    /** @type { number|undefined } */
    this.row = undefined;
  }
}

/** Класс для создания объектов, содержащих объекты с данными 
 * @template T
*/
class DBItemsObject {

  /** Возвращает новый объект DbItemsObject отфильтрованный по результатам вызова callback для каждого элемент
   * @param { ( item: dbItem<T>, hashKey: string, object: dbItemsObject <T> ) => boolean } callback
   * @param { any } [thisArg] - значение this
   * @returns { dbItemsObject <T> }
  */
  filter(callback, thisArg = undefined) {
    if (typeof callback !== 'function') throw TypeError(`${callback} is not a function`);
    /** @type { dbItemsObject <T> } *///@ts-ignore
    const filtered = new DBItemsObject();
    let hash;
    for (hash in this) {
      if (callback.call(thisArg, this[hash], hash, this)) filtered[hash] = this[hash];
    }

    return filtered
  }


  /**
   * @param { ( item: dbItem<T>, hashKey: string, object: dbItemsObject <T> ) => void } callback - Функция принимающая dataItem и вызываемая для каждого элемента
   * @param { any } [thisArg]
   * @returns { void }
  */
  forEach(callback, thisArg) {
    if (typeof callback !== 'function') throw TypeError(`${callback} is not a function`);
    let hash;
    for (hash in this) callback.call(thisArg, this[hash], hash, this)
  }
  /** Возвращает количество элементов в объекте
   * @returns { number }
  */
  get length() {
    return Object.keys(this).length
  }
  /** Возвращает новый объект DbItemsObject с результатом вызова callback для каждого элемента
   * @param { ( item: dbItem<T>, hashKey: string, object: dbItemsObject <T> ) => any } callback
   * @param { any } [thisArg]
   * @returns { { [hash: string]: any } }
  */
  map(callback, thisArg) {
    if (typeof callback !== 'function') throw TypeError(`${callback} is not a function`);
    const mapped = {};
    let hash, item, key;
    for (hash in this) {
      item = new DBItem;
      for (key in this[hash]) {
        item[key] = this[hash][key]
      }
      mapped[hash] = callback.call(thisArg, item, hash, this)
    }
    return mapped
  }

  /** Вызывает callback для каждого элемента, передавая результат выполнения функции в следую итерацию в аргумент acc
   * @param { ( acc: any, item: dbItem <T>, hashKey: string, object: dbItemsObject <T> ) => any } callback
   * @param { any } initialValue - Начальное значение. Атрибут обязателен.
   * @returns { any }
  */
  reduce(callback, initialValue) {
    if (typeof callback !== 'function') throw TypeError(`${callback} is not a function`);
    let hash, acc = initialValue;
    for (hash in this) {
      // @ts-ignore
      acc = callback(acc, this[hash], hash, this)
    }
    return acc
  }
}

/*export { DB, DBItem, DBItemsObject };*/