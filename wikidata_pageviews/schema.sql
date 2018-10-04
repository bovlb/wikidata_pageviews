CREATE TABLE IF NOT EXISTS qid_hourly_views
(
    qid INT NOT NULL,
    hour DATETIME NOT NULL,
    views INT NOT NULL,
    PRIMARY KEY (qid,hour),
    INDEX hour_qid (hour,qid)
);

CREATE TABLE IF NOT EXISTS hours (
    file VARCHAR(80) NOT NULL PRIMARY KEY,
    hour DATETIME NOT NULL UNIQUE KEY,
    processed DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration INT NOT NULL,
    views INT NOT NULL,
    max_qid INT NOT NULL,
    n_qids INT NOT NULL
);