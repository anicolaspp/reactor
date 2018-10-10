package com.github.anicolaspp.clicksimulator;

import java.util.stream.Stream;

public interface LinkSelector {
    
    Stream<Link> getRandomLinksStream(LinksRepository links);
}


