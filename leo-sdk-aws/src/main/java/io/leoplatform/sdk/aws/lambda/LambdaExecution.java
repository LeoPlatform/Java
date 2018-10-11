package io.leoplatform.sdk.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context;

public interface LambdaExecution {
    void run(Context context);
}
