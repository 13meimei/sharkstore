//创建数据库和表的range提交方法
function creatRange() {
	//库名
	var dbName = $("#db_name").val();
	//表名
	var name = $("#name").val();
	
	var startkey = $("#startkey").val();
	
	var endkey = $("#endkey").val();
	
	var peernum = $("#peernum").val();
	
    //执行ajax提交
    	$.ajax({
    		url:"/metadata/createRange?db_name="+dbName+"&name="+name+"&startkey="+startkey+"&endkey="+endkey+"&peernum="+peernum,
    		type:"post",
    		contentType:"application/json;charset=utf-8",
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