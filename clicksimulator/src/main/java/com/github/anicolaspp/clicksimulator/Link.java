package com.github.anicolaspp.clicksimulator;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class Link {
    String value;
    
    boolean isHot;
}
