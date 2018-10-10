package com.github.anicolaspp.clicksimulator;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
class LinkMessage {
    String path;
    
    boolean isHot;
}
