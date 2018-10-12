package com.github.anicolaspp.clicksimulator;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
class LinkMessage {
    String path;
    
    boolean isHot;
}
