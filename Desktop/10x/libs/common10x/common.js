
/**Сокращение для Logger.log */
function log(something) {
  Logger.log(something);
}

function requireScopes() {
  ScriptApp.requireScopes(ScriptApp.AuthMode.FULL, [
    'https://www.googleapis.com/auth/spreadsheets'
    , 'https://www.googleapis.com/auth/script.external_request'
  ]);
}

function showMessage(title = "", messages = []) {
  let ui = SpreadsheetApp.getUi(); // Same variations.
  if (title !== "") ui.alert(title, messages.join("\n"), ui.ButtonSet.OK);
  else ui.alert(messages.join("\n"), ui.ButtonSet.OK);
}

/**
 * @param { string } message
 * @param { string } title
 * @param { number } timeout - seconds
 */
function showToast(message, title = undefined, timeout = undefined) {
  if (title) {
    if (timeout) {
      SpreadsheetApp.getActive().toast(message, title, timeout);
      return
    }
    SpreadsheetApp.getActive().toast(message, title);
    return
  } else {
    SpreadsheetApp.getActive().toast(message);
  }
}

/** Возвращает букву колонки на основе номера 
 * @private
 * @param { number } colNum - номер колонки 
 * @returns { string } */
function letter(colNum) {
  if (colNum < 1) throw new Error(`Wrong column number (${colNum})`)
  let remain,
    string = "";
  if (colNum < 27) return String.fromCharCode(64 + colNum);
  do {
    remain = colNum % 26;
    if (remain == 0) {
      remain = 26;
      colNum -= remain
    }
    colNum = Math.floor(colNum / 26);
    string = String.fromCharCode(64 + remain) + string;
  } while (colNum > 0);
  return string;
}

/** 
 * @param { string|Date } date 
 * @param { Object } options 
 * @param { number } [options.years=0]
 * @param { number } [options.months=0]
 * @param { number } [options.days=0]
 * @param { number } [options.hours=0]
 * @param { number } [options.minutes=0]
 * @returns { Date }
*/
function offsetDate(date, { years = 0, months = 0, days = 0, hours = 0, minutes = 0 }) {
  const newDate = new Date(date);
  if (years !== 0) newDate.setFullYear(newDate.getFullYear() + years);
  if (months !== 0) newDate.setMonth(newDate.getMonth() + months);
  if (days !== 0) newDate.setDate(newDate.getDate() + days);
  if (hours !== 0) newDate.setHours(newDate.getHours() + hours);
  if (minutes !== 0) newDate.setMinutes(newDate.getMinutes() + minutes);
  return newDate;
}

/**
 * Отправка id скрипта в таблицу
 * @param { string } scriptId - id скрипта
 * @param { string } SS_ID - spreadsheet_id
 */
function updateScriptId(scriptId, SS_ID) {
  let getDataRequestPayload = {
      method: 'patch',
      url: `/ss/script_id/${SS_ID}`,
      payload: {
          script_id: scriptId
      }
  };
  return btlzApi(getDataRequestPayload);
}
