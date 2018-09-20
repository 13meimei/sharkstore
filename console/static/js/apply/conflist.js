(function () {
    /**
     * configure 详情列表展示
     */
    $('#confLists').bootstrapTable({
        url: getUrl(),
        striped: true,
        cache: false,
        pagination: true,
        pageNumber: 1,
        pageSize: 10,
        pageList: [10, 25, 50, 100],
        sidePagination: "server",
        clickToSelect: true,
        iconSize: 'outline',
        toolbar: '#confListsToolbar',
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
            {field: '', radio: true, align: 'center'},
            {field: 'k', title: 'key', align: 'center'},
            {field: 'v', title: '配置', align: 'center'},
            {field: 'version', title: '版本', align: 'center'},
            {field: 'extend', title: '扩展', align: 'center'},
            {
                field: 'create_time', title: '过期时间', align: 'center',
                formatter: function (value, row, index) {
                    //调用下面方法进行时间戳格式化
                    if(!hasText(value) || value == 0){
                        return "";
                    }
                    return formatDate((new Date(value)), "yyyy-MM-dd hh:mm:ss");
                }
            },
            {
                field: 'upd_time', title: '更新时间', align: 'center',
                formatter: function (value, row, index) {
                    //调用下面方法进行时间戳格式化
                    if(!hasText(value) || value == 0){
                        return "";
                    }
                    return formatDate((new Date(value)), "yyyy-MM-dd hh:mm:ss");
                }
            },
            {field: 'creator', title: '客户端ip', align: 'center'}
        ],
        responseHandler: function (res) {
            if (res.code === 0) {
                return {
                    "total": res.data.total,//当前页面数据
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

function getUrl() {
    return "/configure/queryList?clusterId=" + $('#clusterId').val()
        + "&dbName=" + $('#dbName').val()
        + "&tableName=" + $('#tableName').val();
}