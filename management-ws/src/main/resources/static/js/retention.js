var pageData = {
    catalogName: null,
    userName: null,
    password: null
};

function fillRetention(catalogName, userName, password){
    pageData.catalogName = catalogName;
    pageData.userName = userName;
    pageData.password = password;
    
    catalogManager.getRetention(catalogName, userName, password, function(retention){
        if (retention !== null){
            $.each(retention, function(i, policy){
                addRetentionPolicy(policy.retentionPeriodInDays, policy.retentionDateKey, policy.retentionFilter);
            });
        }else{
            addPolicy();
        }
    });
}

function addPolicy(){
    addRetentionPolicy("", "", "");
}

function savePolicy(){
    var policySet = [];
    $(".retentionDiv").each(function(i){
        var policy = {};
        var period = $(this).find(".period").val();
        var date = $(this).find(".date").val();
        var filter = $(this).find(".filter").val();
        if (filter === ""){
            filter = null;
        }
        policy.retentionPeriodInDays = parseInt(period);
        policy.retentionDateKey = date;
        policy.retentionFilter = filter;
        policySet.push(policy);
    });
    
    catalogManager.saveRetentionPolicy(pageData.catalogName, policySet, pageData.userName, pageData.password, function(response){
        if (response){
            window.location.href = "/catalogs/" + pageData.catalogName;
        }else{
            alert("There was a problem saving your Retention Policy");
        }
    });
}

function addRetentionPolicy(periodInDays, dateKey, filter){
    var container = $("#retentionContainer");
    
    var retDiv = $("<div class='retentionDiv' />");
    var retTable = $("<table class='retentionTable' />");
    
    var row = $("<tr />");
    row.append("<td>Retention Period (days):</td>");
    row.append("<td><input type='text' value='" + periodInDays + "' class='period' /></td>");
    retTable.append(row);
    
    row = $("<tr />");
    row.append("<td>Retention Date Field:</td>");
    row.append("<td><input type='text' value='" + dateKey + "' class='date' /></td>");
    retTable.append(row);
    
    row = $("<tr />");
    row.append("<td>Retention Filter:</td>");
    row.append("<td><input type='text' value='" + (filter === null ? "" : filter) + "' class='filter' /></td>");
    retTable.append(row);
    
    row = $("<tr />");
    row.append("<td>&nbsp;</td>");
    var removeButton = $("<input type='button' value='Remove Policy' />");
    removeButton.click(function(){
        retDiv.remove();
    });
    row.append($("<td />").append(removeButton));
    retTable.append(row);
    
    retDiv.append(retTable);
    container.append(retDiv);
    
}
