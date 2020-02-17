import { render } from '@testing-library/react';
import React from 'react';
import { SetupWebDAV } from '../SetupWebDAV';
import { changeControlValue } from './testutils';

it('can set fields', async () => {
  let ref = React.createRef();
  const { getByTestId } = render(<SetupWebDAV ref={ref} />)

  expect(ref.current.validate()).toBe(false);
  // required
  changeControlValue(getByTestId("control-url"), "some-url");
  expect(ref.current.validate()).toBe(true);
  // optional
  changeControlValue(getByTestId("control-username"), "some-username");
  changeControlValue(getByTestId("control-password"), "some-password");
  expect(ref.current.validate()).toBe(true);

  expect(ref.current.state).toStrictEqual({
    "url": "some-url",
    "username": "some-username",
    "password": "some-password",
  });
});
