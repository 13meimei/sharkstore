var app = angular.module('peers', []);
//集群监控数据获取
app.controller('mypeers', function($rootScope, $scope, $http, $timeout) {
     var rangeId = $('#rangeId').val();
     var clusterId = $('#clusterId').val();
	 var dbName = $("#dbName").val();
     var tableName = $("#tableName").val();
	 $http.get('/range/getPeerInfo?rangeId='+ rangeId + '&tableName='+tableName +'&dbName='+dbName +'&clusterId=' + clusterId).success(function(data){
	 	if(data.code === 0){
			$scope.peerInfo = data.data;
		 }else {
			swal("获取失败", data.msg, "error");
		 }
	 });
	 $scope.updateRange = function(peerId){
        $.ajax({
        		url:"/range/updateRange",
        		type:"post",
        		async: false,
                contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                dataType:"json",
                data:{
                	"dbName":dbName,
                	"tableName":tableName,
                	"rangeId":rangeId,
                	"peerId":peerId,
                	"clusterId":clusterId
                },
        		success: function(data){
        			if(data.code === 0){
        			    swal("range元信息修改成功！", "range元信息修改成功!", "success");
//        			    window.location.reload();
        			}else {
        				swal("range元信息修改失败", data.msg, "error");
        			}
                },
                error: function(res){
                	swal("range元信息修改失败", res, "error");
                }
        	});
	 }
    $scope.replaceRange = function(peerId){
        $.ajax({
            url:"/range/replaceRange",
            type:"post",
            async: false,
            contentType:"application/x-www-form-urlencoded; charset=UTF-8",
            dataType:"json",
            data:{
                "dbName":dbName,
                "tableName":tableName,
                "rangeId":rangeId,
                "peerId":peerId,
                "clusterId":clusterId
            },
            success: function(data){
                if(data.code === 0){
                    swal("range替换成功！", "range替换成功!", "success");
//        			    window.location.reload();
                }else {
                    swal("range替换失败", data.msg, "error");
                }
            },
            error: function(res){
                swal("range替换失败", res, "error");
            }
        });
    }
 });
