function generateTable() {
    var letters = [null,'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','w','v', 
    'x','y','z'];
    var idRow = document.createElement("TR");
    for (var a = 0; a < 28; a++) {

        var col = document.createElement("TD");
        idRow.appendChild(col);
        if (letters[a] == null) {
            col.innerHTML = " ~ ";
        } else {
            col.innerHTML = letters[a];
        }
        if(a == 27){
			col.innerHTML = "TOTAL";

        }
        idRow.appendChild(col);
    }

    document.getElementById('tab').appendChild(idRow);

    for (var a = 1; a < 27; a++) {

        var dataRow = document.createElement('TR');
        var id = document.createElement("TD");
        id.innerHTML = letters[a];
        dataRow.appendChild(id);
        for (var b = 0; b < 27; b++) {
            var id = document.createElement("TD");
            id.innerHTML = "0";
            id.id = letters[a] + letters[b + 1];
            if(b == 26){
            	id.id=letters[a]+"T";
            }
            dataRow.appendChild(id);
        }
        document.getElementById('tab').appendChild(dataRow);

        if(a==26){
    		var totals = document.createElement('TR');
    		for(var i = 0; i < 28; i++){
    			var tmp = document.createElement("TD");
    			tmp.innerHTML = "0";
    			tmp.id = "T"+letters[i];
    			totals.appendChild(tmp);
    			if(i == 0){
    				tmp.innerHTML = "TOTAL";
    			}
    			if(i == 27){
    				tmp.innerHTML = "~";
    			}
    		}
    		 document.getElementById('tab').appendChild(totals);
    	}
    }
}
window.onload = function() {
    var content;
    var content2;
    var fileInputJson = document.getElementById('fileInputJson');
    fileInputJson.addEventListener('change', function(e) {
      
        var file = fileInputJson.files[0];
	console.log(file);

        var reader = new FileReader();
        reader.onload = function(e) {
            content = reader.result;
            
            var data = JSON.parse(content);
            //console.log(data);
            for(var i in data){
            	try{
            	var key = i.split('_');
            	key = key[0]+key[1];
            	key = key.toLowerCase();
            	


            	var item = data[i];
            	console.log(item);

            	document.getElementById(key).innerHTML = item[i];
            }
            catch(err){

            }
            }
            


        }
        reader.readAsText(file);
    });

     var fileInputTotal = document.getElementById('fileInputTotal');
    fileInputTotal.addEventListener('change', function(e) {
    	console.log("WOOT");
        console.log('json reader loaded');
        var file = fileInputTotal.files[0];

        var reader2 = new FileReader();
        reader2.onload = function(e) {
            content2 = reader2.result;
            
            var parsed = JSON.parse(content2);
           

           for(var i in parsed){
           	var key = ""+i;
           	key = key.toLowerCase();
           	key = key+"T";

           	var item = parsed[i];

           	console.log(key);

           		try{

           			document.getElementById(key).innerHTML=item[i];
           			
           		}
           		catch(err){
           		
           		}
           }

           for(var i in parsed){
           	var key = ""+i;
           	key = key.toLowerCase();
           	key = "T"+key;

           	var item = parsed[i];

           	console.log(key);

           		try{

           			document.getElementById(key).innerHTML=item[i];
           			
           		}
           		catch(err){
           		
           		}
           }



     
          
        }
        reader2.readAsText(file);
    });
}
