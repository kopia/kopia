import { render } from '@testing-library/react';
import React from 'react';
import { SetupGCS } from '../SetupGCS';
import { changeControlValue } from './testutils';

it('can set fields', async () => {
  let ref = React.createRef();
  const { getByTestId } = render(<SetupGCS ref={ref} />)

  expect(ref.current.validate()).toBe(false);
  // required
  changeControlValue(getByTestId("control-bucket"), "some-bucket");
  expect(ref.current.validate()).toBe(true);
  // optional
  changeControlValue(getByTestId("control-prefix"), "some-prefix");
  changeControlValue(getByTestId("control-credentialsFile"), "some-credentials-file");
  changeControlValue(getByTestId("control-credentials"), "some-credentials");
  expect(ref.current.validate()).toBe(true);

  expect(ref.current.state).toStrictEqual({
    "bucket": "some-bucket",
    "credentials": "some-credentials",
    "credentialsFile": "some-credentials-file",
    "prefix": "some-prefix",
  });
});
