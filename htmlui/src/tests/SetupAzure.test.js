import { render } from '@testing-library/react';
import React from 'react';
import { SetupAzure } from '../SetupAzure';
import { changeControlValue } from './testutils';

it('can set fields', async () => {
  let ref = React.createRef();
  const { getByTestId } = render(<SetupAzure ref={ref} />)

  expect(ref.current.validate()).toBe(false);
  // required
  changeControlValue(getByTestId("control-container"), "some-container");
  changeControlValue(getByTestId("control-storageAccount"), "some-storageAccount");
  changeControlValue(getByTestId("control-storageKey"), "some-storageKey");
  expect(ref.current.validate()).toBe(true);
  // optional
  changeControlValue(getByTestId("control-prefix"), "some-prefix");
  expect(ref.current.validate()).toBe(true);

  expect(ref.current.state).toStrictEqual({
    "storageAccount": "some-storageAccount",
    "container": "some-container",
    "prefix": "some-prefix",
    "storageKey": "some-storageKey",
  });
});
