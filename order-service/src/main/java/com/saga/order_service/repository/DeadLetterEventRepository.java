package com.saga.order_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.saga.order_service.entity.DeadLetterEvent;

public interface DeadLetterEventRepository extends JpaRepository<DeadLetterEvent, Long> {}
