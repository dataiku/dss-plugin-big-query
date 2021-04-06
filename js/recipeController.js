// Couldn't find a way to separate the controllers/services in their own file
// as there are some problems of loading (The file that creates the module should be loaded before any other
// and I could not find a way to do so)

const app = angular.module('bigQueryUnnest.recipe', []);

app.controller('unnestRecipeController', function ($scope, utils) {

    const updateScopeData = function (data) {
        $scope.inputSchema = data.inputSchema;
        if (data.inputSchema.length > 0) {
            utils.initVariable($scope, 'fieldToKeep', data.inputSchema[0][1]);
        } else {
            utils.initVariable($scope, 'fieldToKeep', '');
        }
    };

    const initVariables = function () {
    };

    const init = function () {
        initVariables();
        utils.retrieveInfoBackend($scope, "uselessForNow", updateScopeData);
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