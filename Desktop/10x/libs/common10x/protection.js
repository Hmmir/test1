
function fetchProtectionAction(action) {
  const response = BtlzApi.fetch({
    url: "/actions",
    payload: { action },
  });
  if (response.getResponseCode() !== 200) {
    throw Error(response.getContentText());
  }
  return JSON.parse(response.getContentText());
}

/**
 * @param { SpreadsheetApp.Spreadsheet } ss
 */
function protect_tech_sheets(ss) {

  const tech_sheets_list_response = fetchProtectionAction("get_tech_sheets_list");
  const tech_sheets_list = Array.isArray(tech_sheets_list_response.result)
    ? tech_sheets_list_response.result
    : [];

  const tech_sheets_editors_response = fetchProtectionAction("get_tech_sheets_editors");
  const tech_sheets_editors = Array.isArray(tech_sheets_editors_response.result)
    ? tech_sheets_editors_response.result
    : [];

  if (!tech_sheets_list) throw Error("Unable to get tech_sheets_list")
  if (!tech_sheets_editors) throw Error("Unable to get tech_sheets_editors")
  // const tech_sheets_list = ["ce", "checklist", "unit_log", "mp_stats", "mp_conv", "config", "today", "cards"];
  for (const sheet_name of tech_sheets_list) {
    const sheet = ss.getSheetByName(sheet_name);
    if (sheet) {
      const protection = sheet.protect()
      protection.addEditors(tech_sheets_editors);
      const not_editors = protection.getEditors().map(e => e.getEmail()).filter(email => !tech_sheets_editors.includes(email));
      protection.removeEditors(not_editors)
      console.log(`sheet '${sheet_name}' protection completed`)
    }
  }
};

/**
 * @param { SpreadsheetApp.Spreadsheet } ss
 */
function set_editors(ss) {
  const tech_sheets_editors_response = fetchProtectionAction("get_tech_sheets_editors");
  const tech_sheets_editors = Array.isArray(tech_sheets_editors_response.result)
    ? tech_sheets_editors_response.result
    : [];
  ss.addEditors(tech_sheets_editors);
}
