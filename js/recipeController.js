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


app.directive('mappingNested', function(Debounce, $timeout) {
        return {
            restrict:'E',
            scope: {
                mapping: '=ngModel',
                onChange: '&',
                noChangeOnAdd: '<',
                addLabel: '@',
                validate: '=?',
                withColor: '=?',
                keepInvalid: '=?',
                required: '<',
                typeAhead: '='
            },
            templateUrl : '/plugins/big-query/resource/templates/mapping-nested.html',
            compile: () => ({
                pre: function (scope, element, attrs) {
                    const textarea = element.find('textarea');
                    textarea.on('keydown', function (e) {
                        let keyCode = e.keyCode || e.which;
                        //tab key
                        if (keyCode === 9) {
                            e.preventDefault();
                            if (!scope.$$phase) scope.$apply(function () {
                                let tabPosition = textarea[0].selectionStart;
                                scope.bulkMapping = scope.bulkMapping.slice(0, tabPosition) + '\t' + scope.bulkMapping.slice(tabPosition);
                                $timeout(function () {
                                    textarea[0].selectionEnd = tabPosition + 1;
                                });
                            });
                        }
                    });
                    scope.changeMode = function () {
                        if (!scope.showBulk) {
                            scope.bulkMapping = scope.mapping.map(m => (m.from === undefined ? '' : m.from) + '\t' + (m.to === undefined ? '' : m.to)).join('\n');
                        }
                        scope.showBulk = !scope.showBulk;
                    };

                    scope.$watch('bulkMapping', Debounce().withDelay(400, 400).wrap(function (nv, ov) {
                            if (!angular.isUndefined(nv)) {
                                if (!nv.length) {
                                    scope.mapping = [];
                                } else {
                                    scope.mapping = nv.split('\n').map(l => {
                                        //regexp to split into no more than 2 parts (everything to the right of a tab is one piece)
                                        const parts = l.split(/\t(.*)/);
                                        return {from: parts[0], to: parts[1]};
                                    });
                                }
                            }
                        })
                    );
                    if (angular.isUndefined(scope.mapping)) {
                        scope.mapping = [];
                    }
                    if (!scope.addLabel) scope.addLabel = 'Add another';
                    if ('preAdd' in attrs) {
                        scope.preAdd = scope.$parent.$eval(attrs.preAdd);
                    } else {
                        scope.preAdd = Object.keys(scope.mapping).length === 0;
                    }
                    if (scope.onChange) {
                        scope.callback = scope.onChange.bind(scope, {mapping: scope.mapping});
                    }
                }
            })

        };
    });
