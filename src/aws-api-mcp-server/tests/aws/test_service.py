import json
import pytest
from ..history_handler import history
from awslabs.aws_api_mcp_server.core.aws.driver import translate_cli_to_ir
from awslabs.aws_api_mcp_server.core.aws.service import (
    execute_awscli_customization,
    get_local_credentials,
    interpret_command,
    is_operation_read_only,
    validate,
)
from awslabs.aws_api_mcp_server.core.common.helpers import as_json
from awslabs.aws_api_mcp_server.core.common.models import (
    AwsApiMcpServerErrorResponse,
    AwsCliAliasResponse,
    CommandMetadata,
    Context,
    Credentials,
    InterpretationMetadata,
    InterpretationResponse,
    IRTranslation,
    ProgramInterpretationResponse,
    ValidationFailure,
)
from awslabs.aws_api_mcp_server.core.metadata.read_only_operations_list import ReadOnlyOperations
from botocore.config import Config
from botocore.exceptions import NoCredentialsError
from tests.fixtures import (
    CLOUD9_DESCRIBE_ENVIRONMENTS,
    CLOUD9_LIST_ENVIRONMENTS,
    CLOUD9_PARAMS_CLI_MISSING_CONTEXT,
    CLOUD9_PARAMS_CLI_NON_EXISTING_OPERATION,
    CLOUD9_PARAMS_CLI_VALIDATION_FAILURES,
    CLOUD9_PARAMS_MISSING_CONTEXT_FAILURES,
    EC2_DESCRIBE_INSTANCES,
    GET_CALLER_IDENTITY_PAYLOAD,
    SSM_LIST_NODES_PAYLOAD,
    T2_EC2_DESCRIBE_INSTANCES_FILTERED,
    TEST_CREDENTIALS,
    patch_boto3,
)
from typing import Any
from unittest.mock import MagicMock, patch


@pytest.mark.parametrize(
    'cli_command,reason,service,operation',
    [
        (
            CLOUD9_PARAMS_CLI_NON_EXISTING_OPERATION,
            "The operation 'list-environments-1' for service 'cloud9' does not exist.",
            'cloud9',
            'list-environments-1',
        ),
    ],
)
def test_interpret_returns_validation_failures(cli_command, reason, service, operation):
    """Test that interpret_command returns validation failures for invalid operations."""
    credentials = Credentials(**TEST_CREDENTIALS)
    response = interpret_command(
        cli_command=cli_command,
        credentials=credentials,
        default_region='us-east-1',
    )
    assert response.response is None
    assert response.validation_failures == [
        ValidationFailure(
            reason=reason,
            context=Context(
                service=service,
                operation=operation,
                parameters=None,
                args=None,
                region=None,
                operators=None,
            ),
        )
    ]


def test_interpret_returns_missing_context_failures():
    """Test that interpret_command returns missing context failures when required parameters are missing."""
    credentials = Credentials(**TEST_CREDENTIALS)
    response = interpret_command(
        cli_command=CLOUD9_PARAMS_CLI_MISSING_CONTEXT,
        credentials=credentials,
        default_region='us-east-1',
    )
    assert response.response is None
    assert response.missing_context_failures == [
        ValidationFailure(
            reason="The following parameters are missing for service 'cloud9' and operation 'create-environment-ec2': '--image-id'",
            context=Context(
                service='cloud9',
                operation='create-environment-ec2',
                parameters=['--image-id'],
                args=None,
                region=None,
                operators=None,
            ),
        )
    ]


@pytest.mark.parametrize(
    'cli,output,event,service,service_full_name,operation',
    [
        (
            'aws cloud9 list-environments',
            CLOUD9_LIST_ENVIRONMENTS,
            ('ListEnvironments', {}, 'us-east-1', 10, 'https://cloud9.us-east-1.amazonaws.com'),
            'cloud9',
            'AWS Cloud9',
            'ListEnvironments',
        ),
        (
            'aws ec2 describe-instances --filters "Name=instance-state-name,Values=running"',
            EC2_DESCRIBE_INSTANCES,
            (
                'DescribeInstances',
                {
                    'Filters': [{'Name': 'instance-state-name', 'Values': ['running']}],
                },
                'us-east-1',
                10,
                'https://ec2.us-east-1.amazonaws.com',
            ),
            'ec2',
            'Amazon Elastic Compute Cloud',
            'DescribeInstances',
        ),
        (
            """aws ec2 describe-instances --query "Reservations[].Instances[?InstanceType=='t2.micro']" """,
            T2_EC2_DESCRIBE_INSTANCES_FILTERED,
            (
                'DescribeInstances',
                {},
                'us-east-1',
                10,
                'https://ec2.us-east-1.amazonaws.com',
            ),
            'ec2',
            'Amazon Elastic Compute Cloud',
            'DescribeInstances',
        ),
        (
            'aws cloud9 describe-environments --environment-ids 7d61007bd98b4d589f1504af84c168de b181ffd35fe2457c8c5ae9d75edc068a',
            CLOUD9_DESCRIBE_ENVIRONMENTS,
            (
                'DescribeEnvironments',
                {
                    'environmentIds': [
                        '7d61007bd98b4d589f1504af84c168de',  # pragma: allowlist secret
                        'b181ffd35fe2457c8c5ae9d75edc068a',  # pragma: allowlist secret
                    ]
                },
                'us-east-1',
                10,
                'https://cloud9.us-east-1.amazonaws.com',
            ),
            'cloud9',
            'AWS Cloud9',
            'DescribeEnvironments',
        ),
        (
            'aws sts get-caller-identity',
            GET_CALLER_IDENTITY_PAYLOAD,
            ('GetCallerIdentity', {}, 'us-east-1', 10, 'https://sts.amazonaws.com'),
            'sts',
            'AWS Security Token Service',
            'GetCallerIdentity',
        ),
        (
            'aws ssm list-nodes --sync-name Luna-Sync --filters Key=IpAddress,Values=1.0.0.1,Type=Equal',
            SSM_LIST_NODES_PAYLOAD,
            (
                'ListNodes',
                {
                    'SyncName': 'Luna-Sync',
                    'Filters': [
                        {
                            'Key': 'IpAddress',
                            'Values': ['1.0.0.1'],
                            'Type': 'Equal',
                        }
                    ],
                },
                'us-east-1',
                10,
                'https://ssm.us-east-1.amazonaws.com',
            ),
            'ssm',
            'Amazon Simple Systems Manager (SSM)',
            'ListNodes',
        ),
    ],
)
def test_interpret_returns_valid_response(
    cli, output: dict[str, Any], event, service, service_full_name, operation
):
    """Test that interpret_command returns a valid response for correct CLI commands."""
    with patch_boto3():
        history.events.clear()
        credentials = Credentials(**TEST_CREDENTIALS)
        response = interpret_command(
            cli_command=cli, default_region='us-east-1', credentials=credentials
        )
        assert response == ProgramInterpretationResponse(
            response=InterpretationResponse(json=as_json(output), error=None, status_code=200),
            failed_constraints=[],
            metadata=InterpretationMetadata(
                service=service,
                operation=operation,
                region_name='us-east-1',
                service_full_name=service_full_name,
            ),
        )
        assert event in history.events


def test_interpret_injects_region():
    """Test that interpret_command injects the correct region into the request."""
    region = 'eu-south-1'
    default_config = Config(region_name=region)
    with patch_boto3():
        with patch('awslabs.aws_api_mcp_server.core.parser.interpretation.Config') as patch_config:
            history.events.clear()
            patch_config.return_value = default_config
            credentials = Credentials(**TEST_CREDENTIALS)
            response = interpret_command(
                cli_command='aws cloud9 describe-environments --environment-ids 7d61007bd98b4d589f1504af84c168de b181ffd35fe2457c8c5ae9d75edc068a',
                credentials=credentials,
                default_region=region,
            )
            assert response.metadata == InterpretationMetadata(
                service='cloud9',
                operation='DescribeEnvironments',
                region_name=region,
                service_full_name='AWS Cloud9',
            )
            event = (
                'DescribeEnvironments',
                {
                    'environmentIds': [
                        '7d61007bd98b4d589f1504af84c168de',  # pragma: allowlist secret
                        'b181ffd35fe2457c8c5ae9d75edc068a',  # pragma: allowlist secret
                    ]
                },
                'eu-south-1',
                60,
                'https://cloud9.eu-south-1.amazonaws.com',
            )
            assert event in history.events


@pytest.mark.parametrize(
    'cli, region',
    [
        (
            'aws cloudwatch list-managed-insight-rules --resource-arn arn:aws:cloudwatch:eu-west-2:123456789012:alarm:AlarmName',
            'eu-west-2',
        ),
        (
            'aws cloudwatch list-managed-insight-rules --resource-arn arn:aws:cloudwatch:eu-west-2:123456789012:alarm:AlarmName --region eu-central-1',
            'eu-central-1',
        ),
        (
            'aws cloudwatch list-managed-insight-rules --resource-arn arn:aws:cloudwatch::123456789012:alarm:AlarmName',
            'us-east-1',
        ),
    ],
)
def test_region_picked_up_from_arn(cli, region):
    """Test that region is correctly picked up from ARN in the CLI command."""
    with patch_boto3():
        credentials = Credentials(**TEST_CREDENTIALS)
        response = interpret_command(
            cli_command=cli,
            default_region='us-east-1',
            credentials=credentials,
        )
        assert response.metadata is not None
        assert response.metadata.region_name == region


def test_validate_success():
    """Test that validate returns success for a valid IR translation."""
    ir = translate_cli_to_ir('aws s3api list-buckets')
    response = validate(ir)
    response_json = json.loads(response.model_dump_json())
    assert response_json['validation_failures'] is None
    assert response_json['missing_context_failures'] is None


@pytest.mark.parametrize(
    'cli_command,validate_response',
    [
        (CLOUD9_PARAMS_CLI_NON_EXISTING_OPERATION, CLOUD9_PARAMS_CLI_VALIDATION_FAILURES),
    ],
)
def test_validate_returns_validation_failures(cli_command, validate_response):
    """Test that validate returns expected validation failures for invalid commands."""
    ir = translate_cli_to_ir(cli_command)
    response = validate(ir)
    response_json = json.loads(response.model_dump_json())
    assert response_json == validate_response


def test_validate_returns_missing_context_failures():
    """Test that validate returns missing context failures for incomplete commands."""
    ir = translate_cli_to_ir(CLOUD9_PARAMS_CLI_MISSING_CONTEXT)
    response = validate(ir)
    response_json = json.loads(response.model_dump_json())
    assert response_json == CLOUD9_PARAMS_MISSING_CONTEXT_FAILURES


@pytest.mark.parametrize(
    'cli_command,validation_failure_reason',
    [
        (
            'aws ec2 describe-instances --instance-ids abcdefgh',
            (
                "The parameter 'InstanceIds' received an invalid input: "
                'Invalid parameter value: The parameter InstanceIds does not match the ^i-[a-f0-9]{8,17}$ pattern'
            ),
        ),
        (
            'aws ec2 describe-security-groups --group-ids abcdefgh',
            (
                "The parameter 'GroupIds' received an invalid input: "
                'Invalid parameter value: The parameter GroupIds does not match the ^sg-[a-f0-9]{8,17}$ pattern'
            ),
        ),
        (
            'aws ec2 describe-instance-attribute --attribute instanceType --instance-id abcdefgh',
            (
                "The parameter 'InstanceId' received an invalid input: "
                'Invalid parameter value: The parameter InstanceId does not match the ^i-[a-f0-9]{8,17}$ pattern'
            ),
        ),
        (
            'aws ec2 describe-security-group-references --group-id abcdefgh',
            (
                "The parameter 'GroupId' received an invalid input: "
                'Invalid parameter value: The parameter GroupId does not match the ^sg-[a-f0-9]{8,17}$ pattern'
            ),
        ),
        (
            'aws ec2 revoke-security-group-ingress --group-id abcdefgh',
            (
                "The parameter 'GroupId' received an invalid input: "
                'Invalid parameter value: The parameter GroupId does not match the ^sg-[a-f0-9]{8,17}$ pattern'
            ),
        ),
    ],
)
def test_validate_returns_ec2_validation_failures(cli_command, validation_failure_reason):
    """Test that validate returns EC2 validation failures for invalid parameters."""
    ir = translate_cli_to_ir(cli_command)
    response = validate(ir)
    response_json = json.loads(response.model_dump_json())
    validation_failures = response_json['validation_failures']
    assert len(validation_failures) == 1
    assert validation_failures[0]['reason'] == validation_failure_reason


def test_is_operation_read_only_returns_true_for_read_only_operation():
    """Test is_operation_read_only returns True for a read-only operation."""
    ir = IRTranslation(
        command_metadata=CommandMetadata(
            service_sdk_name='s3',
            service_full_sdk_name='Amazon S3',
            operation_sdk_name='list-buckets',
        )
    )

    read_only_operations = ReadOnlyOperations({})
    read_only_operations['s3'] = ['list-buckets']

    result = is_operation_read_only(ir, read_only_operations)

    assert result is True


def test_is_operation_read_only_returns_false_for_non_read_only_operation():
    """Test is_operation_read_only returns False for non-read-only operation."""
    ir = IRTranslation(
        command_metadata=CommandMetadata(
            service_sdk_name='s3',
            service_full_sdk_name='Amazon S3',
            operation_sdk_name='delete-object',
        )
    )

    read_only_operations = ReadOnlyOperations({})
    read_only_operations['s3'] = ['list-buckets']

    result = is_operation_read_only(ir, read_only_operations)

    assert result is False


def test_is_operation_read_only_returns_false_for_unknown_service():
    """Test is_operation_read_only returns False for unknown service."""
    ir = IRTranslation(
        command_metadata=CommandMetadata(
            service_sdk_name='unknown-service',
            service_full_sdk_name='Unknown Service',
            operation_sdk_name='list-buckets',
        )
    )

    read_only_operations = ReadOnlyOperations({})
    read_only_operations['s3'] = ['list-buckets']

    result = is_operation_read_only(ir, read_only_operations)

    assert result is False


def test_is_operation_read_only_raises_error_for_missing_command_metadata():
    """Test is_operation_read_only raises error for missing command metadata."""
    ir = IRTranslation(command_metadata=None)
    read_only_operations = ReadOnlyOperations({})

    with pytest.raises(RuntimeError, match='failed to check if operation is allowed'):
        is_operation_read_only(ir, read_only_operations)


def test_is_operation_read_only_raises_error_for_missing_service_name():
    """Test is_operation_read_only raises error for missing service name."""
    ir = IRTranslation(
        command_metadata=CommandMetadata(
            service_sdk_name='',
            service_full_sdk_name='Amazon S3',
            operation_sdk_name='list-buckets',
        )
    )
    read_only_operations = ReadOnlyOperations({})

    with pytest.raises(RuntimeError, match='failed to check if operation is allowed'):
        is_operation_read_only(ir, read_only_operations)


def test_is_operation_read_only_raises_error_for_missing_operation_name():
    """Test is_operation_read_only raises error for missing operation name."""
    ir = IRTranslation(
        command_metadata=CommandMetadata(
            service_sdk_name='s3', service_full_sdk_name='Amazon S3', operation_sdk_name=''
        )
    )
    read_only_operations = ReadOnlyOperations({})

    with pytest.raises(RuntimeError, match='failed to check if operation is allowed'):
        is_operation_read_only(ir, read_only_operations)


@patch('awslabs.aws_api_mcp_server.core.aws.service.AWS_API_MCP_PROFILE_NAME', 'test')
@patch('awslabs.aws_api_mcp_server.core.aws.service.boto3.Session')
def test_get_local_credentials_success_with_aws_mcp_profile(mock_session_class):
    """Test get_local_credentials returns credentials when available."""
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session

    mock_credentials = MagicMock()
    mock_credentials.access_key = 'test-access-key'
    mock_credentials.secret_key = 'test-secret-key'  # pragma: allowlist secret
    mock_credentials.token = 'test-session-token'

    mock_session.get_credentials.return_value = mock_credentials

    result = get_local_credentials()

    assert isinstance(result, Credentials)
    assert result.access_key_id == 'test-access-key'
    assert result.secret_access_key == 'test-secret-key'  # pragma: allowlist secret
    assert result.session_token == 'test-session-token'
    mock_session_class.assert_called_once_with(profile_name='test')
    mock_session.get_credentials.assert_called_once()


@patch('awslabs.aws_api_mcp_server.core.aws.service.boto3.Session')
def test_get_local_credentials_success_with_default_creds(mock_session_class):
    """Test get_local_credentials returns credentials when available."""
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session

    mock_credentials = MagicMock()
    mock_credentials.access_key = 'test-access-key'
    mock_credentials.secret_key = 'test-secret-key'  # pragma: allowlist secret
    mock_credentials.token = 'test-session-token'

    mock_session.get_credentials.return_value = mock_credentials

    result = get_local_credentials()

    assert isinstance(result, Credentials)
    assert result.access_key_id == 'test-access-key'
    assert result.secret_access_key == 'test-secret-key'  # pragma: allowlist secret
    assert result.session_token == 'test-session-token'
    mock_session_class.assert_called_once()
    mock_session.get_credentials.assert_called_once()


@patch('awslabs.aws_api_mcp_server.core.aws.service.boto3.Session')
def test_get_local_credentials_raises_no_credentials_error(mock_session_class):
    """Test get_local_credentials raises NoCredentialsError when credentials are None."""
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    mock_session.get_credentials.return_value = None

    with pytest.raises(NoCredentialsError):
        get_local_credentials()

    mock_session_class.assert_called_once()
    mock_session.get_credentials.assert_called_once()


@patch('awslabs.aws_api_mcp_server.core.aws.service.driver')
def test_execute_awscli_customization_success(mock_driver):
    """Test execute_awscli_customization returns AwsCliAliasResponse on successful execution."""
    mock_driver.main.return_value = None

    with patch('awslabs.aws_api_mcp_server.core.aws.service.StringIO') as mock_stringio:
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.getvalue.return_value = 'bucket1\nbucket2\n'
        mock_stderr.getvalue.return_value = ''
        mock_stringio.side_effect = [mock_stdout, mock_stderr]

        result = execute_awscli_customization('aws s3 ls')

        assert isinstance(result, AwsCliAliasResponse)
        assert result.response == 'bucket1\nbucket2\n'
        assert result.error == ''

        mock_driver.main.assert_called_once_with(['s3', 'ls'])


@patch('awslabs.aws_api_mcp_server.core.aws.service.driver')
def test_execute_awscli_customization_error(mock_driver):
    """Test execute_awscli_customization returns AwsApiMcpServerErrorResponse on exception."""
    mock_driver.main.side_effect = Exception('Invalid command')

    result = execute_awscli_customization('aws invalid command')

    assert isinstance(result, AwsApiMcpServerErrorResponse)
    assert result.error is True
    assert result.detail == "Error while executing 'aws invalid command': Invalid command"

    mock_driver.main.assert_called_once_with(['invalid', 'command'])


@patch('awslabs.aws_api_mcp_server.core.aws.service.driver.main')
@patch('awslabs.aws_api_mcp_server.core.aws.service.AWS_API_MCP_PROFILE_NAME', None)
def test_profile_not_added_when_env_var_none(mock_main):
    """Test that profile is not added when AWS_API_MCP_PROFILE_NAME is None."""
    execute_awscli_customization('aws s3 ls')

    # Verify profile was not added to args
    args = mock_main.call_args[0][0]
    assert '--profile' not in args


@patch('awslabs.aws_api_mcp_server.core.aws.service.driver.main')
@patch('awslabs.aws_api_mcp_server.core.aws.service.AWS_API_MCP_PROFILE_NAME', 'test-profile')
def test_profile_added_when_env_var_set(mock_main):
    """Test that profile is added when AWS_API_MCP_PROFILE_NAME is set."""
    execute_awscli_customization('aws s3 ls')

    # Verify profile was added to args
    args = mock_main.call_args[0][0]
    assert '--profile' in args
    profile_index = args.index('--profile')
    assert args[profile_index + 1] == 'test-profile'


@patch('awslabs.aws_api_mcp_server.core.aws.service.driver.main')
@patch('awslabs.aws_api_mcp_server.core.aws.service.AWS_API_MCP_PROFILE_NAME', 'test-profile')
def test_profile_not_added_if_present_for_customizations(mock_main):
    """Test that profile is not added when one is already present."""
    execute_awscli_customization('aws s3 ls --profile different')

    # Verify profile was added to args
    args = mock_main.call_args[0][0]
    assert '--profile' in args
    profile_index = args.index('--profile')
    assert args[profile_index + 1] == 'different'
