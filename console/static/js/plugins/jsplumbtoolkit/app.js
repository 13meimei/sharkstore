(function () {

    jsPlumbToolkit.ready(function () {

        // get a new jsPlumb Toolkit instance to use.
        var toolkit = window.toolkit = jsPlumbToolkit.newInstance({
            beforeStartDetach:function() { return false; }
        });

        var mainElement = document.querySelector("#jtk-demo-multiple-hierarchy"),
            canvasElement = mainElement.querySelector(".jtk-demo-canvas"),
            miniviewElement = mainElement.querySelector(".miniview");

        //
        // use event delegation to attach event handlers to
        // remove buttons. This callback finds the related Node and
        // then tells the toolkit to delete it and all of its descendants.
        //
        jsPlumb.on(canvasElement, "tap", ".delete", function (e) {
            var info = toolkit.getObjectInfo(this);
            var selection = toolkit.selectDescendants(info.obj, true);
            toolkit.remove(selection);
        });

        //
        // use event delegation to attach event handlers to
        // add buttons. This callback adds an edge from the given node
        // to a newly created node, and then the layout is refreshed automatically.
        //
        jsPlumb.on(canvasElement, "tap", ".add", function (e) {
            // this helper method can retrieve the associated
            // toolkit information from any DOM element.
            var info = toolkit.getObjectInfo(this);
            // get data for a random node.
            var n = jsPlumbToolkitDemoSupport.randomNode();
            // add the node to the toolkit
            var newNode = toolkit.addNode(n);
            // and add an edge for it from the current node.
            toolkit.addEdge({source: info.obj, target: newNode});
        });

        // render the data using a hierarchical layout
        var renderer = toolkit.render({
            container: canvasElement,
            consumeRightClick: false,
            layout: {
                type: "Hierarchical",
                parameters: {
                    orientation: "horizontal",
                    padding: [60, 60]
                }
            },
            miniview: {
                container:miniviewElement,
                initiallyVisible: false
            },
            lassoFilter: ".controls, .controls *, .miniview, .miniview *",
            lassoInvert:true,
            events: {
                canvasClick: function (e) {
                    toolkit.clearSelection();
                },
                modeChanged: function (mode) {
                    jsPlumb.removeClass(jsPlumb.getSelector("[mode]"), "selected-mode");
                    jsPlumb.addClass(jsPlumb.getSelector("[mode='" + mode + "']"), "selected-mode");
                }
            },
            elementsDraggable: false,
            jsPlumb:{
                Anchors: ["Bottom", "Top"],
                Connector: [ "StateMachine", { curviness: 10 } ],
                PaintStyle: { lineWidth: 1, stroke: '#89bcde' },
                HoverPaintStyle: { stroke: "#FF6600", lineWidth: 3 },
                Endpoints: [
                    [ "Dot", { radius: 2 } ],
                    "Blank"
                ],
                EndpointStyle: { fill: "#89bcde" },
                EndpointHoverStyle: { fill: "#FF6600" }
            }
        });

        // pan mode/select mode
        jsPlumb.on(".controls", "tap", "[mode]", function () {
            renderer.setMode(this.getAttribute("mode"));
        });

        // on home button click, zoom content to fit.
        jsPlumb.on(".controls", "tap", "[reset]", function () {
            toolkit.clearSelection();
            renderer.zoomToFit();
        });

        var hierarchy = jsPlumbToolkitDemoSupport.randomHierarchy(3, 2);
        var hierarchy2 = jsPlumbToolkitDemoSupport.randomHierarchy(3, 2);
        var hierarchy3 = jsPlumbToolkitDemoSupport.randomHierarchy(2, 4);

        function mergeHierarchy(h, h2, prefix) {
            h2.edges.forEach(function(e) {
                e.data.id = prefix + e.data.id;
                e.source = prefix + e.source;
                e.target = prefix + e.target;
                h.edges.push(e);
            });

            h2.nodes.forEach(function(e) {
                e.id = prefix + e.id;
                e.name = prefix + e.name;
                h.nodes.push(e);
            });
        }

        mergeHierarchy(hierarchy, hierarchy2, "2:");
        mergeHierarchy(hierarchy, hierarchy3, "3:");



        toolkit.load({
            data: hierarchy,
            onload: renderer.zoomToFit
        });

        var datasetView = new jsPlumbSyntaxHighlighter(toolkit, ".jtk-demo-dataset");

    });

})();