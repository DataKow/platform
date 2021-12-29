package org.datakow.apps.managementws;

import org.datakow.catalogs.metadata.webservice.configuration.EnableMetadataCatalogWebServiceClient;
import org.datakow.catalogs.metadata.webservice.configuration.MetadataCatalogWebServiceClientConfigurationProperties;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 *
 * @author kevin.off
 */
@Controller
@EnableMetadataCatalogWebServiceClient
public class Endpoints {
    
    MetadataCatalogWebServiceClientConfigurationProperties props;
    
    @Autowired
    public Endpoints(MetadataCatalogWebServiceClientConfigurationProperties props){
        this.props = props;
    }
    
    @RequestMapping(value="/", method=RequestMethod.GET)
    public String index(Model model){
        return "index";
    }
    
    @RequestMapping(value = "/catalogs", method = RequestMethod.GET)
    public String catalogs(Model model) throws IOException{
        model.addAttribute("baseUrl", "http://" + props.getMetadataCatalogWebserviceHost() + ":" + props.getMetadataCatalogWebservicePort());
        model.addAttribute("userName", props.getWebserviceUsername());
        model.addAttribute("password", props.getWebservicePassword());
        return "catalogs"; 
    }
    
    @RequestMapping(value = "/catalogs/{catalogName}", method = RequestMethod.GET)
    public String catalog(Model model, @PathVariable("catalogName") String catalogName) throws IOException{
        model.addAttribute("catalogName", catalogName);
        model.addAttribute("baseUrl", "http://" + props.getMetadataCatalogWebserviceHost() + ":" + props.getMetadataCatalogWebservicePort());
        model.addAttribute("userName", props.getWebserviceUsername());
        model.addAttribute("password", props.getWebservicePassword());
        return "catalog";
    }
    
    @RequestMapping(value = "/catalogs/{catalogName}/indexes/{indexName}/edit")
    public String editIndex(Model model, 
            @PathVariable("catalogName") String catalogName,
            @PathVariable("indexName") String indexName){
        
        model.addAttribute("catalogName", catalogName);
        model.addAttribute("indexName", indexName);
        model.addAttribute("baseUrl", "http://" + props.getMetadataCatalogWebserviceHost() + ":" + props.getMetadataCatalogWebservicePort());
        model.addAttribute("userName", props.getWebserviceUsername());
        model.addAttribute("password", props.getWebservicePassword());
        return "editIndex";
    }
    
    @RequestMapping(value = "/catalogs/{catalogName}/indexes/new")
    public String createNewIndex(Model model, 
            @PathVariable("catalogName") String catalogName){
        
        model.addAttribute("catalogName", catalogName);
        model.addAttribute("baseUrl", "http://" + props.getMetadataCatalogWebserviceHost() + ":" + props.getMetadataCatalogWebservicePort());
        model.addAttribute("userName", props.getWebserviceUsername());
        model.addAttribute("password", props.getWebservicePassword());
        return "editIndex";
    }
    
    @RequestMapping(value = "/catalogs/{catalogName}/retention")
    public String modifyRetention(Model model, 
            @PathVariable("catalogName") String catalogName){
        
        model.addAttribute("catalogName", catalogName);
        model.addAttribute("baseUrl", "http://" + props.getMetadataCatalogWebserviceHost() + ":" + props.getMetadataCatalogWebservicePort());
        model.addAttribute("userName", props.getWebserviceUsername());
        model.addAttribute("password", props.getWebservicePassword());
        return "retention";
    }
    
}
