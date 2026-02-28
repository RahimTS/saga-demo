package com.saga.order_service.dto;

import jakarta.validation.constraints.*;
import lombok.Data;
import java.math.BigDecimal;

@Data
public class CreateOrderRequest {

    @NotBlank(message = "customerId is required")
    private String customerId;

    @NotBlank(message = "productId is required")
    private String productId;

    @NotNull
    @Min(value = 1, message = "quantity must be at least 1")
    private Integer quantity;

    @NotNull
    @DecimalMin(value = "0.01", message = "amount must be greater than 0")
    private BigDecimal amount;

    // Simulation flags — default false so normal requests just work
    private boolean forcePaymentFailure = false;
    private boolean forceStockFailure = false;
}