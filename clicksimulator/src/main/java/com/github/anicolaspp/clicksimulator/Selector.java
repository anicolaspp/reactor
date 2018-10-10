package com.github.anicolaspp.clicksimulator;

import lombok.val;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Selector implements LinkSelector {
    
    private Random rnd = new Random();
    
    @Override
    public Stream<Link> getRandomLinksStream(LinksRepository links) {
        return Stream.iterate(selectLink(links), (l) -> selectLink(links));
    }
    
    private Link selectLink(LinksRepository links) {
        val li = links.getLinks().collect(Collectors.toList());
        
        return li.get(rnd.nextInt(li.size()));
    }
}
