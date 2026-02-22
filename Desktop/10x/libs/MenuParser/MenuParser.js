class MenuParser {
  /**
   * @param { menu } config
   */
  static parseMenu(config) {
    const ui = SpreadsheetApp.getUi();
    let menu = ui.createMenu(config.title);
    if ("items" in config && Array.isArray(config.items)) {
      for (const i of config.items) {
        if (i.type === "item") menu = menu.addItem(i.title, i.functionName)
        if (i.type === "separator") menu = menu.addSeparator();
        if (i.type === "subMenu") menu = menu.addSubMenu(this.parseMenu(i));
      }
    }
    if (config.type === "subMenu") return menu;
    if (config.type === "menu") menu.addToUi()
  }

  /**
   * Привязывает функции для которых описаны functionProperties к функциям в globalThis по имени указанном в functionName
   * @param { menu } config 
   */
  static bindFunctions(config, gThis) {
    if (!gThis) gThis = globalThis
    if ("items" in config && Array.isArray(config.items)) {
      for (const i of config.items) {
        if (i.type === "item" && i.functionProperties !== undefined) {
          if ("function" in i.functionProperties) {
            gThis[i.functionName] = () => {
              const properties = i.functionProperties
              const params = this.parseParams(properties.params)
              gThis[properties.function](params)
            }
          }
        }
        if (i.type === "subMenu") this.bindFunctions(i, gThis)
      }
    }
  }

  static parseParams(params) {
    const parsed = {}
    for (const key in params) {
      if (key === "ssId" && params[key] === "current") { parsed[key] = SpreadsheetApp.getActiveSpreadsheet().getId(); continue }
      parsed[key] = params[key]
    }
    return parsed;
  }

}
/**
 * @param { menu } config
 */
function parseMenu(config) { MenuParser.parseMenu(config) }
/**
 * @param { menu } config
 */
function bindFunctions(config, gThis) { MenuParser.bindFunctions(config, gThis) }
/** @typedef { Object } menu
 *  @property { "menu" } type
 *  @property { string } title
 *  @property { Array<menu|subMenu|menuItem|menuSeparator> } items
*/
/** @typedef { Object } subMenu
 *  @property { "subMenu" } type
 *  @property { string } title
 *  @property { Array<menu|menuItem|menuSeparator> } items
*/
/** @typedef { Object } menuItem
 *  @property { "item" } type
 *  @property { string } title
 *  @property { string } functionName - функци для привязки к пункту меню
 *  @property { Object } [functionProperties]  - нараметры для создания функции в globalThis для привязки к пункту меню
 *  @property { string } [functionProperties.function]  - уже существующая функция для вызова
 *  @property { { [key: string]: any} } functionProperties.params - объект с параметрами для вызова функции
 * 
*/
/** @typedef { Object } menuSeparator
 *  @property { "separator" } type
*/
/**
 * @param { menu|menuItem|menuSeparator } item
 */