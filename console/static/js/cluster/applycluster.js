//集群申请
var app = angular.module('myCluster', []);
//添加erp用户的逻辑
app.controller('userLableCtrl', ['$scope', '$element', '$compile', function($scope, $element, $compile){
  var i = 1;
  $scope.addUser = function (user) {
  var htmlText = '<div class="form-group" id="user_' + i + '">' +
				    '<label class="col-sm-2 control-label">普通用户：</label>' +
				    '<div class="col-sm-1">' +
				           '<input type="text" class="form-control" id="clusterUser" name="clusterUser" placeholder="请输入erp账号"/>' +
				    '</div>' +
				    '<i class="fa fa-minus ml20" ng-click="removeUser(' + i + ')"></i>' + 
				  '</div>';
    //追加用户信息    
    var tpl = $compile(htmlText);
    var e = tpl($scope);
    $('#one').append(e);
    i++;
  };
  //删除添加的用户
  $scope.removeUser = function (obj) {
    $("#user_" + obj).remove();
  };
}]);




