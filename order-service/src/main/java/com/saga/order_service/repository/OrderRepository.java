package com.saga.order_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.saga.order_service.entity.Order;

import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {
}
