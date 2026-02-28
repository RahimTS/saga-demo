package com.saga.payment.repository;

import com.saga.payment.entity.DeadLetterEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeadLetterEventRepository extends JpaRepository<DeadLetterEvent, Long> {}