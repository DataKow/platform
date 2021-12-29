var pageData = {
    catalogName: null,
    userName: null,
    password: null
};

function fillCatalog(catalogName, userName, password){
    pageData.catalogName = catalogName;
    pageData.userName = userName;
    pageData.password = password;
    
    if (catalogName !== "new"){
        catalogManager.getCatalog(catalogName, userName, password, function(catalog){
            $("#tblIndexes").html("");
            if (catalog !== null){
                $.each(catalog.Indexes, function(i, index){
                    addIndexToPage(index);
                });
                $.each(catalog["Retention-Policy"], function(i, policy){
                    addRetentionPolicy(policy);
                });
            }else{
                alert("Error retrieving catalog " + catalogName);
            }
        });
    }
}

function deleteIndex(indexName){
    if (confirm("Are you sure you want to delete the index: " + indexName)){
        catalogManager.deleteIndex(pageData.catalogName, indexName, pageData.userName, pageData.password, function(response){
            if (response){
                fillCatalog(pageData.catalogName, pageData.userName, pageData.password);
            }
        });
    }
}

function deleteCatalog(){
    if (confirm("Are you sure you want to delete the catalog: " + pageData.catalogName)){
        catalogManager.deleteCatalog(pageData.catalogName, pageData.userName, pageData.password, function(response){
            if (response){
                window.location.href = "/catalogs";
            }else{
                alert("There was an error deleting your catalog");
            }
        });
    }
}

function addRetentionPolicy(policy){
    var pDiv = $("<div class='policyDiv'></div>");
    var pTable = $("<table class='policyTable' />");
    
    var row = $("<tr />");
    row.append("<td>Retention Period (days):</td>");
    row.append("<td>" + policy.retentionPeriodInDays + "</td>");
    pTable.append(row);
    
    row = $("<tr />");
    row.append("<td>Retention Date Field</td>");
    row.append("<td>" + policy.retentionDateKey + "</td>");
    pTable.append(row);
    
    row = $("<tr />");
    row.append("<td>Retention Filter:</td>");
    row.append("<td>" + policy.retentionFilter + "</td>");
    pTable.append(row);
    
    pDiv.append(pTable);
    $("#tblRetention").append(pDiv);
}

function addIndexToPage(index){
    var indexDiv = $("<div class='indexDiv'></div>");
    var indexTable = $("<table class='indexTable' />");
    
    var row = $("<tr />");
    row.append("<td class='nameCol'>Name:</td>");
    row.append("<td class='nameCol'>" + index.name + "</td>");
    indexTable.append(row);

    var unique = index.unique ? "true" : "false";

    row = $("<tr />");
    row.append("<td>Unique:</td>");
    row.append("<td>" + unique + "</td>");
    indexTable.append(row);

    row = $("<tr />");
    row.append("<td class='fieldsCol'>Fields</td>");
    var col = $("<td />");
    $.each(index.indexFields, function(i, field){
        addIndexFieldToPage(field, col);
    });
    row.append(col);

    indexTable.append(row);

    row = $("<tr />");
    row.append("<td>Actions:</td>");
    var editButton = $("<div class='action'><a href='/catalogs/" + pageData.catalogName + "/indexes/" + index.name + "/edit'>Edit</a></div>");
    var deleteButton = $("<div class='action'>Delete</div>").click(function(){
        deleteIndex(index.name);
    });
    row.append($("<td />").append(editButton).append(deleteButton));
    indexTable.append(row);

    indexDiv.append(indexTable);
    $("#tblIndexes").append(indexDiv);
}

function addIndexFieldToPage(field, container){
    var tbl = $("<table class='indexField'></table>");

    var row = $("<tr />");
    row.append("<td>key:</td>");
    row.append("<td>" + field.key + "</td>");
    tbl.append(row);


    row = $("<tr />");
    row.append("<td>direction:</td>");
    if (field.type === "DEFAULT"){
        row.append("<td>" + field.direction + "</td>");
    }else{
        row.append("<td>&nbsp;</td>");
    }
    tbl.append(row);


    row = $("<tr />");
    row.append("<td>type:</td>");
    row.append("<td>" + field.type + "</td>");
    tbl.append(row);

    container.append(tbl);
}

function makeACopy(){
    
    var catalogName = $("#txtDestCatalog").val();
    catalogManager.createCatalog(catalogName, pageData.userName, pageData.password, false, function(success){
        if (success){
            catalogManager.getCatalog(pageData.catalogName, pageData.userName, pageData.password, function(catalog){
                if (catalog.Indexes != null && catalog.Indexes.length > 0){
                    catalogManager.saveIndexes(catalogName, catalog.Indexes, pageData.userName, pageData.password, function(response){
                        if (response !== null){
                            window.location.href = "/catalogs/" + catalogName;
                        }else{
                            alert("Something went wrong creating the indexes.");
                        }
                    });
                }
            });
        }else{
            alert("Something went wrong creating the catalog.");
        }
    });
    
}
    

