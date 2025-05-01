import {
  ILanguageServerPlugin,
  IConnectionDriverConstructor,
} from "@sqltools/types";
import PrestoDriver from "./driver";
import { DRIVER_ALIASES } from "./../constants";

const PrestoDriverPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server
        .getContext()
        .drivers.set(value, PrestoDriver as IConnectionDriverConstructor);
    });
  },
};

export default PrestoDriverPlugin;
