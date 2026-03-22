package com.jonathanfoucher.kafkaproducer.data.dto;

import lombok.Getter;
import lombok.Setter;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

@Getter
@Setter
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class MovieKey {
    private Long id;

    @Override
    public String toString() {
        return String.format("{ id=%s }", id);
    }
}
