
function last(s){
  var result = "";
  for (i =  s.length - 1; i >= 0; i--){
    var c = s.charAt(i);
    if ((c == " ") || (c == "　")){
      if (result != ""){
        break;
      }
    }else{
      result = c + result;
    }
   }
   return result;
}

function showrel(s){
  var xhr = new XMLHttpRequest();
  xhr.open("GET" , "/cgi-bin/suggest.py?p=" + s);
  xhr.onload = function (e) {
    if ((xhr.readyState == 4) && (xhr.status === 200)){
      document.suggest.relnoun.value = xhr.responseText
    }
  }
  xhr.send()
}

function changed() {
  var init = "";
  setTimeout(function () {
    raw = document.main.keyword.value
    tmp = last(raw);
    if (tmp == "") {
      document.suggest.relnoun.value = "" 
    }else if (raw != init){
      showrel(tmp);
      init = raw;
    }
    setTimeout(arguments.callee , 350)
  },350);
}

