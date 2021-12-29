var catalogManager = {
    baseUrl: null
};

catalogManager.init = function(url){
    catalogManager.baseUrl = url;
};

catalogManager.util = {
    buildUrl: function(catalogName, additionalPath, parameters){
        var url = catalogManager.baseUrl + "/catalogs";
        if (catalogName !== undefined && catalogName !== null && catalogName !== ""){
            url = url + "/" + catalogName;
        }
        if (additionalPath !== undefined && additionalPath !== null && additionalPath !== ""){
            url = url + "/" + additionalPath;
        }
        if (parameters !== undefined && parameters !== null && parameters !== ""){
            url = url + "?" + parameters;
        }
        return url;
    }
};

catalogManager.getAllCatalogNames = function(userName, password, callback){
    var params = "properties=Doc.Catalog-Identifier";
    var url = this.util.buildUrl("catalogs", "records", params);
    $.ajax({
        url: url,
        type: "GET",
        dataType: "json",
        beforeSend: function(xhr){
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(data){
            if (typeof callback === "function"){
                var names = [];
                $.each(data, function(i, catalog){
                    names.push(catalog.Doc["Catalog-Identifier"]);
                });
                callback(names.sort(function(a,b){
                    return a.toLowerCase().localeCompare(b.toLowerCase());
                }));
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(null);
        }
    });
};


catalogManager.getCatalog = function(catalogName, userName, password, callback){
    var url = this.util.buildUrl(catalogName, null, "includeIndexes=true");
    $.ajax({
        url: url,
        type: "GET",
        dataType: "json",
        beforeSend: function(xhr){
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(data){
            if (typeof callback === "function"){
                callback(data);
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(null);
        }
    });
};

catalogManager.getIndex = function(catalogName, indexName, userName, password, callback){
    var url = this.util.buildUrl(catalogName, "indexes/" + indexName + "/", null);
    $.ajax({
        url: url,
        type: "GET",
        dataType: "json",
        beforeSend: function(xhr){
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(data){
            if (typeof callback === "function"){
                callback(data);
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(null);
        }
    });
};

catalogManager.getRetention = function(catalogName, userName, password, callback){
    var url = this.util.buildUrl(catalogName, "retention", null);
    $.ajax({
        url: url,
        type: "GET",
        dataType: "json",
        beforeSend: function(xhr){
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(data){
            if (typeof callback === "function"){
                callback(data);
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(null);
        }
    });
};

catalogManager.createCatalog = function(catalogName, userName, password, indexStorage, callback){
    var parameters = "Catalog-Identifier=" + catalogName;
    if (indexStorage != null && indexStorage === true){
        parameters = parameters + "&indexStorageObject=true";
    }else{
        parameters = parameters + "&indexStorageObject=false"
    }
    var url = this.util.buildUrl(null, null, parameters);
    $.ajax({
        url: url,
        type: "POST",
        dataType: "json",
        beforeSend: function(xhr){
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(){
            if (typeof callback === "function"){
                callback(true);
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(false);
        }
    });
};

catalogManager.deleteCatalog = function(catalogName, userName, password, callback){
    var url = this.util.buildUrl(catalogName, null, null);
    $.ajax({
        url: url,
        type: "DELETE",
        dataType: "json",
        beforeSend: function(xhr){
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(){
            if (typeof callback === "function"){
                callback(true);
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(false);
        }
    });
};

catalogManager.saveIndexes = function(catalogName, indexes, userName, password, callback){
    var url = this.util.buildUrl(catalogName, "indexes", null);
    $.ajax({
        traditional: true,
        url: url,
        type: "POST",
        dataType: "json",
        data: JSON.stringify(indexes),
        beforeSend: function(xhr){
            xhr.setRequestHeader("Content-Type", "application/json")
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(){
            if (typeof callback === "function"){
                callback(true);
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(false);
        }
    });
};

catalogManager.saveRetentionPolicy = function(catalogName, policy, userName, password, callback){
    var url = this.util.buildUrl(catalogName, "retention", null);
    $.ajax({
        traditional: true,
        url: url,
        type: "POST",
        dataType: "json",
        data: JSON.stringify(policy),
        beforeSend: function(xhr){
            xhr.setRequestHeader("Content-Type", "application/json")
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(){
            if (typeof callback === "function"){
                callback(true);
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(false);
        }
    });
};

catalogManager.deleteIndex = function(catalogName, indexName, userName, password, callback){
    var url = this.util.buildUrl(catalogName, "indexes/" + indexName, null);
    $.ajax({
        url: url,
        type: "DELETE",
        dataType: "json",
        beforeSend: function(xhr){
            xhr.setRequestHeader('Authorization',
                'Basic ' + btoa(userName + ":" + password));
        },
        success: function(){
            if (typeof callback === "function"){
                callback(true);
            }
        }
    }).fail(function(){
        if (typeof callback === "function"){
            callback(false);
        }
    });
};



