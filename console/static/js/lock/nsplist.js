/**
 * lock Namespace申请列表展示
 */
var app = angular.module('nspApplyInfo', []);
//lock申请数据获取
app.controller('applyList', function($rootScope, $scope, $http, $timeout) {
	 $http.get('/lock/namespace/queryList').success(function(data){
	 	if(data.code === 0){
			$scope.applyList = data.data;
		 }else {
			swal("获取lock namespace列表失败", data.msg, "error");
		 }
	 });
    //能看到，就能修改
    $scope.updateOwner = function(namespace){
        swal({
                title: "编辑",
                text: "修改所属人",
                type: "input",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "执行",
                closeOnConfirm: false
            },
            function (inputValue) {
                if(!inputValue){
                    swal("请先录入该namespace的所属人！");
                    return
                }
                $.ajax({
                    url:"/lock/namespace/update",
                    type:"post",
                    contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                    dataType:"json",
                    data:{
                        namespace:namespace.namespace,
                        clusterId:namespace.cluster_id,
						applyer:inputValue
                    },
                    success: function(data){
                        if(data.code === 0){
                            swal("更新成功!", data.msg, "success");
                            window.location.href = "/page/lock/viewNamespace";
                        }else {
                            swal("更新失败", data.msg, "error");
                        }
                    },
                    error: function(res){
                        swal("更新namespace的所属人失败", res, "error");
                    }
                });
            });
    };
    $scope.viewCluster = function(namespace){
        $.ajax({
            url:"/cluster/getById",
            type:"post",
            contentType:"application/x-www-form-urlencoded; charset=UTF-8",
            dataType:"json",
            data:{
                clusterId:namespace.cluster_id
            },
            success: function(data){
                if(data.code === 0){
                    var innerhtml = "<table><tr><td>集群id：</td><td>" + data.data.id + "</td></tr>" +
                        "<tr><td>集群名：</td><td>" + data.data.name + "</td></tr>" +
                        // "<tr><td>lock访问地址：</td><td>data.data.id</td></tr>" +
                        "</table>"
                    swal({
                        title: "集群详情",
                        text: innerhtml,
                        html: true,
                        showCancelButton: false
                    });
                }else {
                    swal("查询失败", data.msg, "error");
                }
            },
            error: function(res){
                swal("查询失败", res, "error");
            }
        });
    };
    $scope.viewLock = function(namespace){
		//todo
    };

});


//lock namespace apply
function applyNamespace(){
    window.location.href="/page/lock/applyNamespace";
}