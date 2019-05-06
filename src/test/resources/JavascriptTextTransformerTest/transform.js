function transform(inJson) {
  var obj = JSON.parse(inJson);
  return JSON.stringify(obj);
}

function resolveDestination(inJson) {
  var obj = JSON.parse(inJson);

  if(obj["type"] != null) {
    return obj["type"]
  }
}
