import { render } from '@testing-library/react';
import React from 'react';
import { SetupS3 } from '../SetupS3';
import { changeControlValue } from './testutils';

it('can set fields', async () => {
  let ref = React.createRef();
  const { getByTestId } = render(<SetupS3 ref={ref} />)

  expect(ref.current.validate()).toBe(false);
  // required
  changeControlValue(getByTestId("control-bucket"), "some-bucket");
  changeControlValue(getByTestId("control-accessKeyID"), "some-accessKeyID");
  changeControlValue(getByTestId("control-secretAccessKey"), "some-secretAccessKey");
  changeControlValue(getByTestId("control-endpoint"), "some-endpoint");
  expect(ref.current.validate()).toBe(true);
  // optional
  changeControlValue(getByTestId("control-prefix"), "some-prefix");
  changeControlValue(getByTestId("control-sessionToken"), "some-sessionToken");
  changeControlValue(getByTestId("control-region"), "some-region");
  expect(ref.current.validate()).toBe(true);

  expect(ref.current.state).toStrictEqual({
    "accessKeyID": "some-accessKeyID",
    "bucket": "some-bucket",
    "endpoint": "some-endpoint",
    "prefix": "some-prefix",
    "region": "some-region",
    "secretAccessKey": "some-secretAccessKey",
    "sessionToken": "some-sessionToken",
  });
});
