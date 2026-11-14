package com.deepvision.studio.training;

import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface TrainingJobEventRepository extends JpaRepository<TrainingJobEvent, Long> {
  List<TrainingJobEvent> findByJobIdAndSequenceGreaterThanOrderBySequenceAsc(
      String jobId,
      long afterSequence,
      Pageable pageable
  );

  Optional<TrainingJobEvent> findFirstByJobIdOrderBySequenceAsc(String jobId);

  @Modifying
  @Transactional
  @Query("delete from TrainingJobEvent e where e.jobId = :jobId and e.sequence <= :maxSequenceInclusive")
  void deleteByJobIdAndSequenceLessThanEqual(
      @Param("jobId") String jobId,
      @Param("maxSequenceInclusive") long maxSequenceInclusive
  );

  @Modifying
  @Transactional
  @Query("delete from TrainingJobEvent e where e.jobId = :jobId")
  void deleteByJobId(@Param("jobId") String jobId);
}
