package com.saga.inventory.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "inventory", schema = "inventory_schema")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Inventory {

    @Id
    private UUID id;

    @Column(name = "product_id", nullable = false, unique = true)
    private String productId;

    @Column(nullable = false)
    private Integer quantity;   // Total stock available

    @Column(nullable = false)
    private Integer reserved;   // Currently reserved (awaiting payment confirmation)

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    // Available stock = quantity - reserved
    public int getAvailableQuantity() {
        return this.quantity - this.reserved;
    }

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = Instant.now();
    }
}