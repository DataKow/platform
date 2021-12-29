var pageData = {
    userName: null,
    password: null
};

function fillCatalogNames(userName, password){
    pageData.userName = userName;
    pageData.password = password;
    catalogManager.getAllCatalogNames(userName, password, function(names){
        if (names !== null){
            $.each(names, function(i, name){
                var floater = $("<div class='floater'></div>");
                floater.append("<a href='/catalogs/" + name + "'>" + name + "</a>");
                $("#catalogs").append(floater);
            });
        }else{
            alert("Error retrieving catalog names");
        }
    });
}

function createCatalog(){
    var catalogName = $("#txtCatalogName").val();
    if (catalogName !== null && catalogName !== ""){
        catalogManager.createCatalog(catalogName, pageData.userName, pageData.password, $("#chkIndexStorageObject").is(":checked"), function(response){
            if (response){
                window.location.href = "/catalogs/" + catalogName;
            }else{
                alert("There was an error creating your catalog");
            }
        });
    }
}
