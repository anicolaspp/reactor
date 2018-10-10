package com.github.anicolaspp.clicksimulator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class Respository implements LinksRepository {
    
    
    private List<Link> links;
    
    
    public Respository() {
        
        links = new ArrayList<>();
        
        links.add(Link.builder().value("/home").isHot(true).build());
        links.add(Link.builder().value("/about").isHot(false).build());
        links.add(Link.builder().value("/products").isHot(false).build());
        links.add(Link.builder().value("/products/1").isHot(false).build());
        links.add(Link.builder().value("/products/2").isHot(true).build());
        links.add(Link.builder().value("/products/3").isHot(false).build());
        links.add(Link.builder().value("/products/4").isHot(true).build());
        links.add(Link.builder().value("/products/5").isHot(false).build());
    }
    
    
    @Override
    public Stream<Link> getLinks() {
        return links.stream();
    }
}
