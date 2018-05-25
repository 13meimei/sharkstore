
var app = angular.module('topologyInfo', []);
//集群拓扑missing列表
app.controller('missList', function($rootScope, $scope, $http, $timeout) {
	var dbName = $("#dbName").val();
	var tableName = $("#tableName").val();
	//集群id
	var clusterId = $('#clusterId').val();
    $.ajax({
        url:"/table/topology/missing",
        type:"post",
        async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
            "dbName":dbName,
            "tableName":tableName,
            "tableName":tableName,
            "clusterId":clusterId
        },
        success: function(data){
            if(data.code === 0){
                $scope.missList = data.data;
            }else {
                swal("获取拓扑失败", data.msg, "error");
            }

        },
        error: function(res){
            swal("获取拓扑失败", res, "error");
        }
    });

 	$scope.createRange = function(miss){
        $.ajax({
            url:"/table/topology/create",
            type:"post",
            contentType:"application/x-www-form-urlencoded; charset=UTF-8",
            dataType:"json",
            data:{
                "clusterId":clusterId,
                "dbName":   dbName,
                "tableName":tableName,
				"startKey": miss.start_key,
				"endKey":   miss.end_key,
                "batchFlag": false
            },
            success: function(data){
                if(data.code === 0){
                    swal("拓扑填充成功!", data.msg, "success");
                }else {
                    swal("拓扑填充失败", data.msg, "error");
                }
            },
            error: function(res){
                swal("拓扑填充失败", res, "error");
            }
        });
 	}
});

function batchCreateRange() {
    $.ajax({
        url:"/table/topology/create",
        type:"post",
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
            "clusterId":$('#clusterId').val(),
            "dbName":   $('#dbName').val(),
            "tableName":$('#tableName').val(),
            "batchFlag": true
        },
        success: function(data){
            if(data.code === 0){
                swal("拓扑批量填充成功!", data.msg, "success");
            }else {
                swal("拓扑批量填充失败", data.msg, "error");
            }
        },
        error: function(res){
            swal("拓扑批量填充失败", res, "error");
        }
    });
}