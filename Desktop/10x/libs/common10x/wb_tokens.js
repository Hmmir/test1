/**
 * Извлекает payload из токена
 * @param {string} jwt
 * @param {string[]} keys
 * @return {[number|string]}
 * @customfunction
 */
function PARSEJWT(jwt, keys) {
  if (Array.isArray(jwt)) {
    if (jwt[0].length > 1) throw new Error("range must have one column width")
  }
  const tokens = [jwt].flat(2)
  keys = [keys].flat(2)
  const result = []
  for (const token of tokens) {
    const payload = parse_token(token)
    const rowArray = [];
    for (const key of keys) {
      if (key in payload) {
        rowArray.push(payload[key])
      } else {
        rowArray.push("")
      }
    }
    result.push(rowArray);
    debugger
  }

  return result;
}

/**
 * @typedef { Object }
 * @property { string } sid
 * @property { string } id
 * @property { number } iid
 * @property { number } oid
 * @property { number } uid
 * @property { number } s
 * @property { string } ent
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
 * Извлекает payload из токена
 * @param {string} token
 * @return {[number|string]}
 * @customfunction
 */
function parse_token(token) {
  const payload = {};
  if (token) {
    const decoded = Utilities.newBlob(Utilities.base64Decode(token.split('.')[1])).getDataAsString();
    Object.assign(payload, JSON.parse(decoded));
    // payload = {
    //   "ent": 1,
    //   "exp": 1717812517,
    //   "id": "b960f76b-c231-4eb0-9827-75b75b04b5fa",
    //   "iid": 39361438,
    //   "oid": 373935,
    //   "s": 1073741926,
    //   "sid": "d511f1aa-897d-4dde-833a-2133a84bb291",
    //   "uid": 39361438
    // }
    if (payload.exp) payload.exp = Utilities.formatDate(new Date(payload.exp * 1000), "+0000", "yyyy-MM-dd HH:mm:ss");
    if (payload.s) {
      const s = payload.s;
      payload.content = (s & 2) ? true : false
      payload.analytics = (s & 4) ? true : false
      payload.prices = (s & 8) ? true : false
      payload.marketplace = (s & 16) ? true : false
      payload.statistics = (s & 32) ? true : false
      payload.adverts = (s & 64) ? true : false
      payload.questions = (s & 128) ? true : false
      payload.recommendations = (s & 256) ? true : false
      payload.read_only = (s & 1073741824) ? true : false
    }
  }
  return payload;
}