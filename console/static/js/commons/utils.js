function hasText(text){
    var result = true;
    if(typeof text == "undefined" || text == null || $.trim(text) == ""){
        result = false;
    }
    return result;
}