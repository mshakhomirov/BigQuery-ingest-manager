/* eslint-disable no-magic-numbers */

class IngestManagerError {
    constructor(code, message) {
        this.code = code;
        this.message = message;
    }
}

const NotFoundErrors = {
    FILE_FORMAT_NOT_FOUND: new IngestManagerError(1, 'ERROR: unknown file format'),
    JOB_RESULT_UNKNOWN: new IngestManagerError(2, 'ERROR: unknown job_result'),
};

const IngestionErrors = {
    DUPLICATION_ATTEMPT: new IngestManagerError(3, 'ERROR: duplication attempt'),

};

module.exports = {
    IngestManagerError,
    NotFoundErrors,
    IngestionErrors,
};
