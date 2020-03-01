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
  changeControlValue(getByTestId("control-username"), "some-username");
  changeControlValue(getByTestId("control-keyfile"), "some-keyfile");
  changeControlValue(getByTestId("control-knownHostsFile"), "some-knownHostsFile");
  expect(ref.current.validate()).toBe(true);
  expect(ref.current.state).toStrictEqual({
    "host": "some-host",
    "username": "some-username",
    "keyfile": "some-keyfile",
    "knownHostsFile": "some-knownHostsFile",
    "path": "some-path",
    "port": 22,
  });

  // now enter key data instead of key file, make sure validation triggers along the way
  changeControlValue(getByTestId("control-keyData"), "some-keyData");
  expect(ref.current.validate()).toBe(false);
  changeControlValue(getByTestId("control-keyfile"), "");
  expect(ref.current.validate()).toBe(true);
  changeControlValue(getByTestId("control-knownHostsData"), "some-knownHostsData");
  expect(ref.current.validate()).toBe(false);
  changeControlValue(getByTestId("control-knownHostsFile"), "");
  expect(ref.current.validate()).toBe(true);

  expect(ref.current.state).toStrictEqual({
    "host": "some-host",
    "username": "some-username",
    "keyfile": "",
    "keyData": "some-keyData",
    "knownHostsFile": "",
    "knownHostsData": "some-knownHostsData",
    "path": "some-path",
    "port": 22,
  });
});
