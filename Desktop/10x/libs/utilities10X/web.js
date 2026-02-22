function sendToWeb(data) {
    return common10x.btlzApi({
      url: "/actions",
      payload: data,
    });
}

// function sendToWeb(data) {
//   processGroupCalculations(data)
// }
