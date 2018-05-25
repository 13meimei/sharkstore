部署说明：
    grafana跟console独立部署，监控模板的编辑在grafana，展示在fbase-console中嵌入

    grafana免登陆配置
    在#enable anonymous access下面增加enabled = true

开发说明：
    嵌入grafana的panel的url，参照http://docs.grafana.org/reference/sharing/


运行说明：
    go run main.go --config=../conf/console.conf
or
    go build -o console
    ./console --config=../conf/console.conf


