import { render } from '@testing-library/react';
import React from 'react';
import { SetupSFTP } from '../SetupSFTP';
import { changeControlValue } from './testutils';

it('can set fields', async () => {
  let ref = React.createRef();
  const { getByTestId } = render(<SetupSFTP ref={ref} />)

  expect(ref.current.validate()).toBe(false);
  // required
  changeControlValue(getByTestId("control-host"), "some-host");
  changeControlValue(getByTestId("control-port"), "22");
  changeControlValue(getByTestId("control-path"), "some-path");
  changeControlValue(getByTestId("control-keyfile"), "some-keyfile");
  expect(ref.current.validate()).toBe(true);

  expect(ref.current.state).toStrictEqual({
    "host": "some-host",
    "keyfile": "some-keyfile",
    "path": "some-path",
    "port": 22,
  });
});
