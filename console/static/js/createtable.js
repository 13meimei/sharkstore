//添加tables列
function addColumn() {
	//初始化行index
	var _len = $('#tablesColumn').children().length - 1;
	//添加时候判断行长度
	if (_len > 20) {
		alert("最多添加20列");
		return false;
	}
	//添加行
	$("#tablesColumn")
			.append(
					"<div class=\"col-sm-12\" id="+ _len+ ">"
							+ "<div class=\"col-md-1\">"
							+ "<input id=\"columnsName\" name=\"columnsName\" type=\"text\" placeholder=\"列名\" class=\"form-control required\" maxlength=\"128\" onblur=\"checkRepetition()\">"
							+ " </div>"
							+ "<div class=\"col-md-1\" align=\"center\">"
							+ "<select id=\"dataType\" name=\"dataType\" data-placeholder=\"类型\"style=\"height: 30px\" tabindex=\"2\">"
							+ "<option value=\"\">请选择数据类型</option>"
							+ "<option value=\"tinyint\" hassubinfo=\"true\">Tinyint</option>"
							+ "<option value=\"smallint\" hassubinfo=\"true\">Smallint</option>"
							+ "<option value=\"int\" hassubinfo=\"true\">Int</option>"
							+ "<option value=\"bigint\" hassubinfo=\"true\">BigInt</option>"
							+ "<option value=\"float\" hassubinfo=\"true\">Float</option>"
							+ "<option value=\"double\" hassubinfo=\"true\">Double</option>"
							+ "<option value=\"varchar\" hassubinfo=\"true\">Varchar</option>"
							+ "<option value=\"binary\" hassubinfo=\"true\">Binary</option>"
							+ "<option value=\"date\" hassubinfo=\"true\">Date</option>"
							+ "<option value=\"timeStamp\" hassubinfo=\"true\">TimeStamp</option>"
							+ "<option value=\"int32\" hassubinfo=\"true\">int32</option>"
							+ "</select>"
							+ "</div>"
							+ "<div class=\"col-md-1\">"
							+ "<input id=\"defaultValue\" name=\"defaultValue\" type=\"text\" placeholder=\"默认值\" class=\"form-control\">"
							+ "</div>"
							+ "<div class=\"col-md-1\" align=\"center\">"
							+ "<input id=\"primaryKey\" name=\"primaryKey\" type=\"checkbox\" value=\"primarykey\" checked=\"\">"
							+ "</div>"
							+ "<div class=\"col-md-1\" align=\"center\">"
							+ "<input id=\"nullable\" name=\"nullable\" type=\"checkbox\" value=\"nullable\" checked=\"\" checked=\"\">"
							+ "</div>"
							+ "<div class=\"col-md-1\" align=\"center\">"
							+ "<input id=\"autoincrement\" name=\"autoincrement\" type=\"checkbox\" value=\"autoincrement\" checked=\"\">"
							+ "</div>"
							+ "<div class=\"col-md-2\">"
							+ "<input id=\"rmark\" name=\"rmark\" type=\"text\" placeholder=\"注释\" class=\"form-control\">"
							+ "</div>"
							+ "<div class=\"col-md-1\" style=\"padding-left:25px\" onclick=\"delColumn("+ _len+ ")\"><li class=\"glyphicon glyphicon-minus\"></li></div>"
							+ "</div>");
}
//判断是否有重复的列名和空
function checkRepetition(){
	var columnsNamesArray = new Array();
	$("input[name=columnsName]").each(function(i){
		  columnsNamesArray[i] = $(this).val();
		});
	for(var i=0;i<columnsNamesArray.length;i++){
		if(columnsNamesArray[i] == ""){
			alert("列名不能为空");
			return;
		}
	}
	var s = columnsNamesArray.join(",")+","; 
	for(var i=0;i<columnsNamesArray.length;i++) { 
	   if(s.replace(columnsNamesArray[i]+",","").indexOf(columnsNamesArray[i]+",")>-1) { 
		   alert("列名有重复:" + columnsNamesArray[i]); 
		   return; 
	   } 
	} 
}
//删除tables列
function delColumn(index) {
	$("div[id='" + index + "']").remove();//删除当前行
	//要剩下最后一行		
}

//创建数据库和表的提交方法
function createTable() {
	//库名
	var dbName = $("#dbName").val();
	var id = $("#id").val();
	//表名
	var name = $("#name").val();
	//range number
	var rangeNumber = $("#rangeNumber").val();
	//副本数量
	var peerNumber = $("#peerNumber").val();
	
	var columnsArray = new Array();
		//循环去取出tables列中的值
		var columnsNamesArray = new Array();
		$("input[name=columnsName]").each(function(i){
			  columnsNamesArray[i] = $(this).val();
			});
		var dataTypeArray = new Array();
		$("select[name=dataType]").each(function(i){
			dataTypeArray[i] = $(this).val();
			});
		var primaryKeyArray = new Array();
		$("input[name=primaryKey]").each(function(i){
			if($(this).is(':checked')){
				var primaryKey = true;
			}else{
				var primaryKey = false;
			}
			primaryKeyArray[i] = primaryKey;
		});
		var default_valueArray = new Array();
		$("input[name=defaultValue]").each(function(i){
			default_valueArray[i] = $(this).val();
		});		
		var nullableArray = new Array();
		$("input[name=nullable]").each(function(i){
			if($(this).is(':checked')){
				var nullable = true;
			}else{
				var nullable = false;
			}
			nullableArray[i] = nullable;
		});
		var autoincrementArray = new Array();
		$("input[name=autoincrement]").each(function(i){
			if($(this).is(':checked')){
				var autoincrement = true;
			}else{
				var autoincrement = false;
			}
			autoincrementArray[i] = autoincrement;
		});
		var rmarkArray = new Array();
		$("input[name=rmark]").each(function(i){
			  rmarkArray[i] = $(this).val();
		});
		for(var i = 0;i < columnsNamesArray.length; i++) {
			columnsArray.push({name: columnsNamesArray[i], datatype: dataTypeArray[i], primarykey: primaryKeyArray[i],defaultvalue:default_valueArray[i],nullable:nullableArray[i],autoincrement:autoincrementArray[i],rmark:rmarkArray[i],index:true});
		}
		//创建db对象，有 数据库名字，和表名字
    var newarryObject = Object();
    	newarryObject = {columns:columnsArray,peernumber:peerNumber,rangenumber:rangeNumber,dbName:dbName,name:name};
    //执行ajax提交
    	$.ajax({
    		contentType:"application/json;charset=utf-8",
	        dataType:"json",
    		url:"/metadata/createTable",
    		type:"post",
    		data:JSON.stringify(newarryObject),
    		success: function(data){
                alert(data.message);
                redirect();
            },
            error: function(res){
                alert(res.responseText);
                redirect();
            }
    	});
}

/**
 * 根据查询结果
 * 跳转到tables页面
 * @param id
 */
function redirect() {
	window.location.href="/metadata/viewCreateTable";
}

function checkOptionsRadios1(){
	if($('#optionsRadios1').is(":checked")){
		alert("ok");
	}else{
		alert("er");
	}
}


function checkPrimaryKeySelect(){
	if($('#primaryKey').is(":checked")){
		$('#primaryKey').prop("checked",true);
	}else{
		$('#primaryKey').removeAttr("checked");
	}
}
function checkNullableSelect(){
	if($('#nullable').is(":checked")){
		$('#nullable').prop("checked",true);
	}else{
		$('#nullable').removeAttr("checked");
	}
}
function checkAutoincrementSelect(){
	if($('#autoincrement').is(":checked")){
		$('#autoincrement').prop("checked",true);
	}else{
		$('#autoincrement').removeAttr("checked");
	}
}