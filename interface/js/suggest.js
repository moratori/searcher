
function last(s){
  var result = "";
  for (i =  s.length - 1; i >= 0; i--){
    var c = s.charAt(i);
    if ((c == " ") || (c == "　")){
        break;
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
    tmp = last(document.main.keyword.value);
    if (tmp == "") {
      document.suggest.relnoun.value = "" 
    }else if (tmp != init){
      showrel(tmp);
      init = tmp;
    }
    setTimeout(arguments.callee , 350)
  },350);
}

