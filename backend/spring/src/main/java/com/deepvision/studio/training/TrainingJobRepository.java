package com.deepvision.studio.training;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TrainingJobRepository extends JpaRepository<TrainingJob, String> {
  Optional<TrainingJob> findByJobIdAndOwner(String jobId, String owner);

  Optional<TrainingJob> findByOwnerAndIdempotencyKey(String owner, String idempotencyKey);

  Optional<TrainingJob> findByIdempotencyScopeKey(String idempotencyScopeKey);

  List<TrainingJob> findByStatusInOrderByCreatedAtAsc(Collection<String> statuses);

  long countByOwnerAndStatusIn(String owner, Collection<String> statuses);

  List<TrainingJob> findByOwnerOrderByCreatedAtDesc(String owner);
}
