function splittingArray(idsArray) {
  let maxSizeGroup = 20;
  let splitArrays = [];
  
  for (let i = 0; i < idsArray.length; i += maxSizeGroup) {
    splitArrays.push(idsArray.slice(i, i + maxSizeGroup));
  }
  return splitArrays;
}

// Получение данных с сервера
function fetchServerData(ssId, nmIds, dateFrom, dateTo, clientId, token) {
  const payload = {
  spreadsheet_id: ssId,
  dataset: {
    name: "wb10xAnalyticsData_v1", 
    values: {
      nm_ids: nmIds,
      date_from: dateFrom
    }
  }
};
  
  try {
    console.log(`Fetching data from server for client ${clientId}, period: ${dateFrom} - ${dateTo}`);
    const response = BtlzApi.fetch({
      url: "/ss/datasets/data",
      payload,
      token,
    });
    const responseCode = response.getResponseCode();
    
    if (responseCode === 200) {
      const data = JSON.parse(response.getContentText());
      console.log(`Received ${Object.keys(data).length} daily records from server`);
      return data;
    } else {
      console.error(`Server error: ${responseCode}`, response.getContentText());
      return null;
    }
  } catch (error) {
    console.error("Error fetching from server:", error);
    return null;
  }
}

// Агрегация daily данных с учетом информации о товаре
function aggregateServerData(serverData) {
  const aggregated = {};
  
  const dataArray = Object.values(serverData);
  
  dataArray.forEach(day => {
    const nmId = day.nm_id;
    
    if (!aggregated[nmId]) {
      aggregated[nmId] = {
        nmID: nmId,
        // ИНФОРМАЦИЯ О ТОВАРЕ 
        vendorCode: day.vendor_code || "",  
        productName: day.title || day.product_name || "",  
        brandName: day.brand_name || "",  
        
        // Остальные поля
        openCardCount: 0,
        addToCartCount: 0,
        addToCartConversionTotal: 0,
        ordersCount: 0,
        ordersSumRub: 0,
        cartToOrderConversionTotal: 0,
        buyoutsCount: 0,
        buyoutsSumRub: 0,
        buyoutPercentTotal: 0,
        cancelCount: 0,
        cancelSumRub: 0,
        daysCount: 0,
        
        stocksWb: 0,
        stocksMp: 0,
        stocksWbTotal: 0,    // Для расчета среднего
        stocksMpTotal: 0,    // Для расчета среднего
        avgPriceRubTotal: 0  // Для расчета средней цены
      };
    }
    
    const acc = aggregated[nmId];
    
    if (!acc.vendorCode && day.vendor_code) acc.vendorCode = day.vendor_code;
    if (!acc.productName && (day.title || day.product_name)) acc.productName = day.title || day.product_name;
    if (!acc.brandName && day.brand_name) acc.brandName = day.brand_name;
    
    acc.openCardCount += Number(day.open_card_count) || 0;
    acc.addToCartCount += Number(day.add_to_cart_count) || 0;
    acc.ordersCount += Number(day.orders_count) || 0;
    acc.ordersSumRub += Number(day.orders_sum_rub) || 0;
    acc.buyoutsCount += Number(day.buyouts_count) || 0;
    acc.buyoutsSumRub += Number(day.buyouts_sum_rub) || 0;
    acc.cancelCount += Number(day.cancel_count) || 0;
    acc.cancelSumRub += Number(day.cancel_sum_rub) || 0;
    
    acc.addToCartConversionTotal += Number(day.add_to_cart_conversion) || 0;
    acc.cartToOrderConversionTotal += Number(day.cart_to_order_conversion) || 0;
    acc.buyoutPercentTotal += Number(day.buyout_percent) || 0;
    
    acc.avgPriceRubTotal += Number(day.orders_sum_rub) || 0;
    
    acc.stocksWb = Number(day.stocks_wb) || 0;
    acc.stocksMp = Number(day.stocks_mp) || 0;
    
    acc.stocksWbTotal += Number(day.stocks_wb) || 0;
    acc.stocksMpTotal += Number(day.stocks_mp) || 0;
    
    acc.daysCount++;
  });
  
  Object.values(aggregated).forEach(item => {
    // Средняя конверсия в корзину
    item.addToCartConversion = Number(item.daysCount > 0 ? 
      item.addToCartConversionTotal / item.daysCount : 0);
    
    item.cartToOrderConversion = Number(item.daysCount > 0 ? 
      item.cartToOrderConversionTotal / item.daysCount : 0);
    
    item.buyoutPercent = Number(item.daysCount > 0 ? 
      item.buyoutPercentTotal / item.daysCount : 0);
    
    item.avgPriceRub = Number(item.ordersCount > 0 ? 
      item.ordersSumRub / item.ordersCount : 0);
    
    item.avgOrdersCountPerDay = Number(item.daysCount > 0 ? 
      item.ordersCount / item.daysCount : 0);
  });
  
  return Object.values(aggregated);
}

// Вспомогательная функция для расчета динамики
function calculateDynamics(current, previous) {
  if (!previous || previous === 0) return "";
  return ((current - previous) / previous * 100).toFixed(2);
}

// Основная функция обновления через сервер
function web_app_wb_analytics_nmreport_refresh_v2(postData) {
  debugger
  if (!postData.ssId) throw new Error("ssId is required");
  if (!postData.analyticsSheetName) throw new Error("analyticsSheetName is required");

  // Получение токенов с сервера
  const url = "/ss/wb/token/get";
  const payload = { "spreadsheet_id": postData.ssId };
  
  const response = BtlzApi.fetch({ url, payload });
  if (response.getResponseCode === "401") throw new Error("401 Unauthorized");

  const wb_tokens = JSON.parse(response.getContentText());
  if (wb_tokens === undefined) throw new Error("wb_tokens is undefined");

  // Получение nmId по токенам и sid
  const nmIdsAndTokens = getNmIdsAndTokens(wb_tokens, postData.ssId);
  const { ssId, analyticsSheetName } = postData;
  const sheet = SpreadsheetApp.openById(ssId).getSheetByName(analyticsSheetName);

  // Определение периодов
  const endDate = new Date();
  endDate.setHours(0, 0, 0, 0);
  const dateTo = Utilities.formatDate(endDate, Session.getScriptTimeZone(), "yyyy-MM-dd");

  const startDate = new Date(endDate);
  const days = postData.days || 7;
  startDate.setDate(startDate.getDate() - days);
  const dateFrom = Utilities.formatDate(startDate, Session.getScriptTimeZone(), "yyyy-MM-dd");

  // Предыдущий период для сравнения
  const prevStartDate = new Date(startDate);
  prevStartDate.setDate(prevStartDate.getDate() - days);
  const prevDateFrom = Utilities.formatDate(prevStartDate, Session.getScriptTimeZone(), "yyyy-MM-dd");

  console.log(`Текущий период: ${dateFrom} - ${dateTo}`);
  console.log(`Предыдущий период: ${prevDateFrom} - ${dateFrom}`);

  const allData = [];
  
  // Обрабатываем каждый набор nmIds
  for (let tokenGroup of Object.values(nmIdsAndTokens)) {
    const nmIds = tokenGroup.nmIds;
    const clientId = tokenGroup.sid; // Используем sid как client_id
    const token = tokenGroup.token;
    
    if (!nmIds || nmIds.length === 0) {
      console.log("No nmIds for token group:", tokenGroup);
      continue;
    }
    
    console.log(`Processing ${nmIds.length} nmIds for client ${clientId}`);
    
    // Разбиваем на группы по 20 (если нужно)
    const nmIdGroups = splittingArray(nmIds);
    
    for (let nmIdGroup of nmIdGroups) {
      // Получаем данные текущего периода
      const currentData = fetchServerData(ssId, nmIdGroup, dateFrom, dateTo, clientId, token);
      
      if (!currentData || Object.keys(currentData).length === 0) {
        console.log(`No current data received for client ${clientId}`);
        continue;
      }
      
      // Получаем данные предыдущего периода
      const previousData = fetchServerData(ssId, nmIdGroup, prevDateFrom, dateFrom, clientId, token);
      
      // Агрегируем данные
      const aggregatedCurrent = aggregateServerData(currentData);
      const aggregatedPrevious = previousData ? aggregateServerData(previousData) : [];
            
      // Формируем данные для таблицы
      aggregatedCurrent.forEach(currentItem => {
        const previousItem = aggregatedPrevious.find(p => p.nmID === currentItem.nmID);
        
        const dataObj = {
          // Первые 35 полей точно как в оригинале
          begin: dateFrom + " 00:00:00",
          end: dateTo + " 00:00:00",
          vendorCode: currentItem.vendorCode || 0,
          nmID: currentItem.nmID,
          name: currentItem.productName || 0,
          brandName: currentItem.brandName || 0,
          openCardCount: currentItem.openCardCount || 0,
          openCardCountP: previousItem?.openCardCount || 0,
          addToCartCount: currentItem.addToCartCount || 0,
          addToCartCountP: previousItem?.addToCartCount || 0,
          ordersCount: currentItem.ordersCount || 0,
          ordersCountP: previousItem?.ordersCount || 0,
          buyoutsCount: currentItem.buyoutsCount || 0,
          buyoutsCountP: previousItem?.buyoutsCount || 0,
          cancelCount: currentItem.cancelCount || 0,
          cancelCountP: previousItem?.cancelCount || 0,
          addToCartPercent: currentItem.addToCartConversion || 0,
          addToCartPercentP: previousItem?.addToCartConversion || 0,
          cartToOrderPercent: currentItem.cartToOrderConversion || 9,
          cartToOrderPercentP: previousItem?.cartToOrderConversion || 0,
          buyoutsPercent: currentItem.buyoutPercent || 0,
          buyoutsPercentP: previousItem?.buyoutPercent || 0,
          ordersSumRub: currentItem.ordersSumRub || 0,
          ordersSumRubP: previousItem?.ordersSumRub || 0,
          ordersSumRubDynamics: calculateDynamics(currentItem.ordersSumRub, previousItem?.ordersSumRub),
          buyoutsSumRub: currentItem.buyoutsSumRub || 0,
          buyoutsSumRubP: previousItem?.buyoutsSumRub || 0,
          cancelSumRub: currentItem.cancelSumRub || 0,
          cancelSumRubP: previousItem?.cancelSumRub || 0,
          avgPriceRub: currentItem.avgPriceRub || 0,
          avgPriceRubP: previousItem?.avgPriceRub || 0,
          avgOrdersCountPerDay: currentItem.avgOrdersCountPerDay || 0,
          avgOrdersCountPerDayP: previousItem?.avgOrdersCountPerDay || 0,
          stocksWb: currentItem.stocksWb || 0,
          stocksMp: currentItem.stocksMp || 0,
        };
        
        allData.push(Object.values(dataObj));
      });
    }
  }
  
  // Запись в таблицу
  const firstRow = 3;
  let lastRow = sheet.getLastRow();
  
  if (allData.length > 0) {
    console.log(`Writing ${allData.length} rows to sheet`);
    
    if (lastRow >= firstRow && (lastRow - firstRow + 1) > allData.length) {
      const diff = (lastRow - firstRow + 1) - allData.length;
      const dummy = Array.from(allData[0], v => null);
      allData.push(...Array.from({ length: diff }, () => dummy));
    }
    
    // Определяем количество колонок
    const numCols = allData[0] ? allData[0].length : 0;
    
    // Вставка данных
    sheet.getRange(firstRow, 1, allData.length, numCols).setValues(allData);
    
    console.log(`Successfully updated ${allData.length} rows`);
  } else {
    console.log("No data to write");
  }
  
  return {
    success: true,
    message: `Updated ${allData.length} rows`,
    period: `${dateFrom} - ${dateTo}`
  };
}

function getNmIdsAndTokens(wb_tokens, ssId) {
 //Получает токены из scriptProperties
  if (!ssId) throw new Error("ssId is required");
  const container_tokens = []

  wb_tokens.result.forEach(item => {
    if (item.message && item.message === "Unauthorized") throw new Error("Unauthorized");
    if (item.analytics && item.analytics === false) throw new Error("Token does not have Analytics");
    container_tokens.push(item.token)

  })

  //Получаем карточки и группируем по кабинетам (sid)
  /**@type { { [nm_id: string]: { sid: string } } } */
  const cardsDB = new DB({ sheetName: "cards", dataRowFirst: 3, ssId:ssId });
  const cardsItems = cardsDB.getDataItems();

  /**@type { { [sid: string]: { token: string, nmIds: string[] } } } */
  const result = {};
  for (const card of cardsItems) {
    if (!(card.sid in result)) result[card.sid] = { nmIds: [] };
    result[card.sid].nmIds.push(card.nm_id);
  }
    //Извлекаем sid из токенов и привязываем к карточкам по sid
    /**@type { { [sid: string]: string } } */
    const sids__tokens = {};
    for (const wb_token of container_tokens) {
      sids__tokens[parse_token(wb_token)["sid"]] = wb_token;
    }

    for (const sid in result) {
      result[sid].token = sids__tokens[sid];
    }
    return result
  
}


// Тест на реальном клиенте MixmoDa
// function test1() {
//   web_app_wb_analytics_nmreport_refresh_v2({
//     ssId: "14IIW_Nb7qMuHcKnX7O6agBwwcOI_9ExXgVnifw1Z7_s",
//     analyticsSheetName: "ВставкаВП"
//   })
// }


