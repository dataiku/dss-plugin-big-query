// Couldn't find a way to separate the controllers/services in their own file
// as there are some problems of loading (The file that creates the module should be loaded before any other
// and I could not find a way to do so)

const app = angular.module('bigQueryUnnest.recipe', []);

app.controller('unnestRecipeController', function ($scope, utils) {

    const updateScopeData = function (data) {
        console.log(data)
        $scope.inputSchema = data.inputSchema;
    };

    const initVariables = function () {
        utils.initVariable($scope, 'layer_to_retrain', 'last');
    };

    const init = function () {
        initVariables();
        console.log("ok1")
        console.log($scope)
        console.log("ok2")
        utils.retrieveInfoBackend($scope, "uselessForNow", updateScopeData);
        console.log("ok3")
        console.log($scope)
        console.log("ok4")
    };

    init();
});

app.service("utils", function () {
    this.initVariable = function ($scope, varName, initValue) {
        const isVarDefined = $scope.config[varName] !== undefined;
        $scope.config[varName] = isVarDefined ? $scope.config[varName] : initValue;
    };

    this.retrieveInfoBackend = function ($scope, method, updateScopeData) {
        $scope.callPythonDo({method}).then(function (data) {
            updateScopeData(data);
            $scope.finishedLoading = true;
        }, function (data) {
            $scope.finishedLoading = true;
        });
    };

    this.getStylesheetUrl = function (pluginId) {
        return `/plugins/${pluginId}/resource/stylesheets/dl-image-toolbox.css`;
    };

    this.getShowHideAdvancedParamsMessage = function (showAdvancedParams) {
        return showAdvancedParams ? "Hide Model Summary" : "Show Model Summary";
    };
})