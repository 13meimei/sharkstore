(function () {
    /**
     * sql ca 列表展示
     */
    $('#caLists').bootstrapTable({
        url: "/sql/ca/queryList",
        striped: true,      //是否显示行间隔色
        cache: false,       //是否使用缓存，默认为true（*）
        // search: true,       //是否显示表格搜索
        pagination: true,   //是否显示分页（*）
        pageNumber: 1,      //初始化加载第一页，默认第一页,并记录
        pageSize: 10,       //每页的记录行数（*）
        pageList: [10, 25, 50, 100],//可供选择的每页的行数（*）
        sidePagination: "server",//分页方式：client客户端分页，server服务端分页（*）
        clickToSelect: true,
        // showColumns: true,  //是否显示所有的列（选择显示的列）
        // showRefresh: true,  //是否显示刷新按钮
        iconSize: 'outline',
        toolbar: '#caListsToolbar',
        height: 500,
        icons: {
            refresh: 'glyphicon-repeat'
        },
        // 得到查询的参数
        queryParams: function (params) {
            //这里的键的名字和控制器的变量名必须一直，这边改动，控制器也需要改成一样的
            var temp = {
                rows: params.limit,                         //页面大小
                page: (params.offset / params.limit) + 1,   //页码
                sort: params.sort,      //排序列名
                sortOrder: params.order //排位命令（desc，asc）
            };
            return temp;
        },
        columns: [
            {field: '', checkbox: true, align: 'center'},
            {field: 'cluster_id', title: '集群Id', align: 'center'},
            {field: 'user_name', title: '账号', align: 'center'},
            {field: 'password', title: '密码', align: 'center'}
        ],
        responseHandler: function (res) {
            if (res.code === 0) {
                return {
                    "total": res.data.total,//总页数
                    "rows": res.data.data   //数据
                };
            } else {
                swal("失败", res.msg, "error");
                return {
                    "total": 0,//总页数
                    "rows": res.data   //数据
                };
            }
        }
    });
})(document, window, jQuery);

//add sql ca
function addCA() {
    $("#clusterId").val("");
    $("#userName").val("");
    $("#password").val("");
    $("#saveButton").attr("style", "");
    $('#caAddModal').modal('show');
}

function saveSqlCA() {
    var clusterId = $("#clusterId").val();
    var userName = $("#userName").val();
    var password = $("#password").val();
    if (!hasText(clusterId) || !hasText(userName) || !hasText(password)) {
        swal("添加", "请填写集群id、账号、密码", "error");
        return
    }
    swal({
        title: "添加sql凭证?",
        type: "warning",
        showCancelButton: true,
        confirmButtonColor: "#DD6B55",
        confirmButtonText: "确认",
        closeOnConfirm: false
    }, function () {
        //执行ajax提交
        $.ajax({
            url: "/sql/ca/add",
            type: "post",
            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
            dataType: "json",
            data: {
                clusterId: clusterId,
                userName: userName,
                password: password
            },
            success: function (data) {
                if (data.code === 0) {
                    swal("添加成功!", data.msg, "success");
                    $('#caAddModal').modal('hide');
                    $('#caLists').bootstrapTable('refresh', {url: '/sql/ca/queryList'});
                } else {
                    swal("添加失败", data.msg, "error");
                }
            },
            error: function (res) {
                swal("添加sql凭证失败", res, "error");
            }
        });
    });
}

function updCA(){
     var selectedRows = $('#caLists').bootstrapTable('getSelections');
     if (selectedRows.length == 0) {
         swal("修改", "请选择要修改的凭证记录", "error");
         return;
     }
    if (selectedRows.length > 1) {
         swal("修改", "只能选择一条凭证记录进行修改", "error");
         return;
     }
     $("#clusterId").val(selectedRows[0].cluster_id);
     $("#userName").val(selectedRows[0].user_name);
     $("#password").val(selectedRows[0].password);
     $("#saveButton").attr("style", "");
     $('#caAddModal').modal('show');
}

//能看到，就能删除，支持批量
function delCA() {
    var selectedRows = $('#caLists').bootstrapTable('getSelections');
    if (selectedRows.length == 0) {
        swal("删除", "请选择要删除的凭证记录", "error");
        return;
    }
    var ids = [];
    for (var i = 0; i < selectedRows.length; i++) {
        ids.push(selectedRows[i].cluster_id);
    }
    if (ids.length == 0) {
        return;
    }
    swal({
            title: "删除操作",
            type: "warning",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "确认",
            closeOnConfirm: false
        },
        function () {
            $.ajax({
                url: "/sql/ca/del",
                type: "post",
                contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                dataType: "json",
                data: {
                    "ids": JSON.stringify(ids)
                },
                success: function (data) {
                    if (data.code === 0) {
                        swal("删除成功！", data.msg, "success");
                        $('#caLists').bootstrapTable('refresh', {url: '/sql/ca/queryList'});
                    } else {
                        swal("删除失败！", data.msg, "error");
                    }
                },
                error: function (res) {
                    swal("删除失败！", "请联系管理员!", "error");
                }
            });
        });
}