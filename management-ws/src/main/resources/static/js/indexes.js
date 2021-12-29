var pageData = {
    catalogName: null,
    indexName: null,
    userName: null,
    password: null
};
function fillIndex(catalogName, indexName, userName, password){
    pageData.catalogName = catalogName;
    pageData.indexName = indexName;
    pageData.userName = userName;
    pageData.password = password;
    if (indexName !== null){
        $("#title").html("Index: " + indexName + "<br />Catalog: " + catalogName);
        catalogManager.getIndex(catalogName, indexName, userName, password, function(index){
            if (index !== null){
                $("#txtName").val(index.name);
                $("#chkUnique").prop("checked", index.unique);
                fillFields(index.indexFields);
            }else{
                alert("Could not retrieve the index: " + indexName + " for catalog: " + catalogName);
            }
        });
    }else{
        $("#title").html("Index: New Index<br />Catalog: " + catalogName);
        addField();
    }
}

function deleteIndex(){
    if (confirm("Are you sure you want to delete the index: " + pageData.indexName)){
        catalogManager.deleteIndex(pageData.catalogName, pageData.indexName, pageData.userName, pageData.password, function(response){
            if (response){
                window.location.href = "/catalogs/" + pageData.catalogName;
            }else{
                alert("Insert Descriptive Error Message Here")
            }
        });
    }
}

function saveIndex(){
    var index = {
        name: $("#txtName").val(),
        unique: $("#chkUnique").prop("checked"),
        indexFields: []
    };
    
    $("#fieldsContainer .fieldTable").each(function(i){
        var field = {
            key: $(this).find(".key").val(),
            direction: $(this).find(".direction").val(),
            type: $(this).find(".type").val()
        };
        index.indexFields.push(field);
    });
    var indexes = [index];
    
    catalogManager.saveIndexes(pageData.catalogName, indexes, pageData.userName, pageData.password, function(response){
        if (response){
            window.location.href = "/catalogs/" + pageData.catalogName;
        }else{
            alert("Insert Descriptive Error Message Here")
        }
    });
}

function fillFields(fields){
    var container = $("#fieldsContainer");
    
    $.each(fields, function(i, field){
        container.append(getFieldTable(field.key, field.direction, field.type));
    });
}

function addField(){
    var container = $("#fieldsContainer");
    container.append(getFieldTable("", "DESC", "DEFAULT"));
}

function getFieldTable(key, direction, type){
    
    var table = $("<table id='" + key.replace(new RegExp("\\.", 'g'), "") + "' class='fieldTable' />");
    var row = $("<tr />");
    row.append("<td>key</td>");
    row.append("<td><input type='text' class='key' value='" + key + "' /></td>");
    table.append(row);
    
    row = $("<tr />");
    row.append("<td>direction</td>");
    var ddDirection = $("<select class='direction' />");
    ddDirection.append($("<option value=''></option>"));
    ddDirection.append($("<option value='DESC'>DESC</option>"));
    ddDirection.append($("<option value='ASC'>ASC</option>"));
    ddDirection.val(direction);
    row.append($("<td />").append(ddDirection));
    table.append(row);
    
    row = $("<tr />");
    row.append("<td>type</td>");
    var ddType = $("<select class='type' />");
    ddType.append($("<option value='DEFAULT'>DEFAULT</option>"));
    ddType.append($("<option value='TEXT'>TEXT</option>"));
    ddType.append($("<option value='GEO'>GEO</option>"));
    ddType.val(type);
    row.append($("<td />").append(ddType));
    table.append(row);
    
    row = $("<tr />");
    row.append("<td>&nbsp;</td>");
    var removeButton = $("<div class='action'>Remove Field</div>");
    removeButton.click(function(){
        $("#" + key.replace(new RegExp("\\.", 'g'), "")).remove();
    });
    row.append($("<td />").append(removeButton));
    
    table.append(row);
    return table;
}