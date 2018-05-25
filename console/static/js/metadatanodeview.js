$(document).ready(function() {
	$.jgrid.defaults.styleUI = 'Bootstrap';
	// Configuration for jqGrid Example 1
	$("#resource_table_list").jqGrid({
		url:'/node/nodeDataGetAll',
		datatype: "json",
		height : "50%",
		autowidth : true,
		shrinkToFit : true,
		rowNum : 10,
		rowList : [ 10, 20, 30 ],
		colNames : [ '序号', 'address', 'heartbeat_addr', 'replicate_addr','操作'],
		colModel : [ 
		    {name : 'id',index : 'id',align : "center",width : 35}, 
			{name : 'address',index : 'address',width : 35,align : "center"}, 
			{name : 'heartbeat_addr',index : 'heartbeat_addr',align : "center",width : 35}, 
			{name : 'replicate_addr',index : 'replicate_addr',align : "center",width : 35},
			{name:'act',index:'act', width:35,sortable:false ,align : "center"}
			],
		pager : "#resource_pager_list",
		viewrecords : true,
		caption : "资源列表",
		sortname : "id",
		sortorder: "desc",
		gridComplete: function(){
			var ids = jQuery("#resource_table_list").jqGrid('getDataIDs');
			for(var i=0;i < ids.length;i++){
				var rowId = ids[i];
				var rowData = jQuery("#resource_table_list").getRowData(rowId);
				var dbId = rowData.id;
				viewTable = "<button id=\"updatenode\" class=\"btn btn-primary\" type=\"button\" title=\"升级\" value=\"升级\" onclick=\"updateNodeId('"+dbId+"');\">升级</button>&nbsp;&nbsp;&nbsp;&nbsp;";
				jQuery("#resource_table_list").jqGrid('setRowData',ids[i],{act:viewTable});
			}	
		},
		hidegrid : false
	});

	// Add responsive to jqGrid
	$(window).bind('resize', function() {
		var width = $('.jqGrid_wrapper').width();
		$('#resource_table_list').setGridWidth(width);
	});
});

//调用后台升级操作
function updateNodeId(nodeId) {
	 swal({
	         title: "您确定要升级吗?",
	         text: "升级后将无法恢复，请谨慎操作！",
	         type: "input",
	         inputPlaceholder: "请输入版本号",
	         showCancelButton: true,
	         confirmButtonColor: "#DD6B55",
	         confirmButtonText: "升级！",
	         cancelButtonText: "再考虑一下…",
	         closeOnConfirm: false
	     },
	     function (inputValue) {
	    	 if (inputValue === false){
	    		 return false;
	    	 }
	    	 if (inputValue === "") {
    		    swal.showInputError("请输入版本号!");
    		    return false;
    		  }
	        	 $.ajax({
	 		        url:"/node/upGrade",
	 		        type:"post",
	 		        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
	 		        dataType:"json",
	 		        data:{
	 		        	"nodeId":nodeId,
	 		        	"serverVersion":inputValue
	 		        },
	 		        success: function(data){
	 		        	if(data.code === 0){
	 		        		swal("升级成功！", "您已经成功升级到最新版本!", "success");
	 			         	//执行升级后删除升级按钮
	 			         	$("#updatenode").remove();
	 		        	}
	 		        },
	 		        error: function(res){
	 		        	swal("升级失败！", "请联系管理员或重新升级!", "error");
	 		        }
	 		    });
	     });
}